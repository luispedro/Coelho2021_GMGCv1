#!/usr/bin/env stack
-- stack script --resolver lts-8.13 --optimize
{-# LANGUAGE LambdaCase, OverloadedStrings, FlexibleContexts, BangPatterns, PackageImports #-}

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified Data.Set as S
import           Control.Concurrent (setNumCapabilities)
import           System.Console.GetOpt
import           System.Environment (getArgs)
import           Data.List (foldl')
import           Safe (atDef)
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class

import Data.Strict.Tuple
import Data.Word
import Data.Int
import Data.DiskHash
import Foreign.Ptr
import Foreign.Storable

import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.Zlib as CZ
import qualified Data.Conduit as C
import           Data.Conduit ((.|))
import           Data.Conduit.Algorithms.Async

import qualified Data.UnionFind as UF
import StorableConduit (readWord32VS, writeWord32VS)
import PairParsing (parseVector)

vAddToUF :: UF.UnionFind -> VU.Vector (Int, Int) -> IO ()
vAddToUF uf = VU.mapM_ $ \(a,b) -> UF.link uf a b

data CmdArgs = CmdArgs
    { ifileArg :: [FilePath]
    , binFileArg :: [FilePath]
    , ofileArg :: FilePath
    , dhtArg :: FilePath
    , blacklistArg :: FilePath
    , verboseArg :: Bool
    , nJobsArg :: Int
    , fullOutputArg :: Bool
    } deriving (Show)



parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' (flip ($)) (CmdArgs [] [] ofile dhtfile "" False 1 False) flags
    where
        flags :: [CmdArgs -> CmdArgs]
        (flags, args, _extraOpts) = getOpt Permute options argv
        ofile = atDef "" args 0
        dhtfile = atDef "" args 1
        options =
            [ Option ['o'] ["output"] (ReqArg (\f c -> c { ofileArg = f }) "FILE") "Output file"
            , Option ['i'] ["input"] (ReqArg (\f c -> c { ifileArg = (f:ifileArg c) }) "FILE") "Input file to check"
            , Option ['b'] ["binary-input"] (ReqArg (\f c -> c { binFileArg = (f:binFileArg c) }) "FILE") "Input file to check"
            , Option ['k'] ["black-list"] (ReqArg (\f c -> c { blacklistArg = f }) "FILE") "Input file to check"
            , Option ['d'] ["dht-file"] (ReqArg (\f c -> c {dhtArg = f }) "FILE") "DHT index"
            , Option ['v'] ["verbose"] (NoArg (\c -> c {verboseArg = True }))  "Verbose"
            , Option ['t'] ["threads"] (ReqArg (\n c -> c { nJobsArg = read n }) "N") "Nr Threads"
            , Option ['f'] ["full"] (NoArg (\c -> c {fullOutputArg = True })) "Full output"
            ]

writeBinUF :: UF.UnionFind -- ^ UF
                -> Bool -- ^ whether to write all outputs
                -> FilePath -- ^ filepath
                -> IO ()
writeBinUF uf@(UF.UnionFind v) writeAll fout = C.runConduitRes $
        pairs
            .| CC.conduitVector 8192
            .| writeWord32VS
            .| CZ.gzip
            .| CB.sinkFileCautious fout
    where
        pairs :: MonadIO m => C.Source m Word32
        pairs = forM_ [0 .. VUM.length v - 1] $ \ix0 -> do
            ix1 <- liftIO $UF.rep uf ix0
            when (writeAll || ix0 /= ix1) $ do
                C.yield (toEnum . fromEnum $ ix0)
                C.yield (toEnum . fromEnum $ ix1)


linker :: (MonadIO m, MonadThrow m) => UF.UnionFind -> C.ConduitM B8.ByteString c m ()
linker uf = readWord32VS 2048 .| linker'
    where
        linker' = C.awaitForever $ \v -> liftIO $
                    forM_ [0 .. VS.length v `div` 2 - 1] $ \ix ->
                            UF.link uf
                                    (fromEnum $ v VS.! (ix * 2 + 0))
                                    (fromEnum $ v VS.! (ix * 2 + 1))



main :: IO ()
main = do
    opts <- parseArgs <$> getArgs
    print opts
    let nthreads = nJobsArg opts
    setNumCapabilities nthreads
    nameh <- htOpenRO (dhtArg opts) 23
    let ngenes = htSizeRO (nameh :: DiskHashRO Int64)
    when (verboseArg opts) $
        putStrLn $ "Loaded ht table: " ++ show ngenes ++ " entries."

    uf <- UF.new ngenes
    blacklist <- case blacklistArg opts of
        "" -> return S.empty
        bfile -> S.fromList <$> C.runConduitRes (CB.sourceFile bfile .| CB.lines .| CC.sinkList)
    forM_ (ifileArg opts) $ \ifile -> do
        when (verboseArg opts) $
            putStrLn ("Processing file '"++ifile++"'")
        C.runConduitRes $
            conduitPossiblyCompressedFile ifile
                .| CB.lines
                .| CC.conduitVector 8192
                .| asyncMapC nthreads (parseVector blacklist nameh)
                .| CL.mapM_ (liftIO . vAddToUF uf)
    forM_ (binFileArg opts) $ \ifile -> do
        when (verboseArg opts) $
            putStrLn ("Processing file '"++ifile++"'")
        C.runConduitRes $
            CB.sourceFile ifile
            .| CZ.multiple CZ.ungzip
            .| linker uf
    writeBinUF uf (fullOutputArg opts) (ofileArg opts)
    when (verboseArg opts) $
        putStrLn "Done."

