#!/usr/bin/env stack
-- stack script --resolver lts-8.13 --optimize
{-# LANGUAGE LambdaCase, OverloadedStrings, FlexibleContexts, BangPatterns, PackageImports #-}

import qualified Data.ByteString as B
import qualified Data.Vector as V
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Storable.Mutable as VSM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Set as S
import           Control.Concurrent (setNumCapabilities)
import           System.Console.GetOpt
import           System.Environment (getArgs)
import           Data.List (foldl')
import           Safe (atDef)
import Control.Monad

import Data.Word
import Data.Int
import Data.DiskHash

import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.Zlib as CZ
import qualified Data.Conduit as C
import           System.IO.SafeWrite (withOutputFile)
import           Data.Conduit ((.|))
import           Data.Conduit.Algorithms.Async

import PairParsing (parseVector)
import StorableConduit (writeWord32VS, readWord32VS)


data CmdArgs = CmdArgs
    { ifileArg :: [FilePath]
    , ofileArg :: FilePath
    , dhtArg :: FilePath
    , ufArg :: FilePath
    , verboseArg :: Bool
    , nJobsArg :: Int
    , blackListArg :: FilePath
    , fullOutputArg :: Bool
    } deriving (Show)



loadUf :: Int -> FilePath -> IO (VU.Vector Int)
loadUf n f = do
    [full] <- C.runConduitRes $
            CB.sourceFile f
                .| CZ.ungzip
                .| readWord32VS (2*n)
                .| CC.sinkList
    return $! VU.generate n (\ix -> fromEnum (full VS.! (ix * 2 + 1)))

parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' (flip ($)) (CmdArgs [] [] ofile dhtfile False 1 "" False) flags
    where
        flags :: [CmdArgs -> CmdArgs]
        (flags, args, _extraOpts) = getOpt Permute options argv
        ofile = atDef "" args 0
        dhtfile = atDef "" args 1
        options =
            [ Option ['o'] ["output"] (ReqArg (\f c -> c { ofileArg = f }) "FILE") "Output file"
            , Option ['i'] ["input"] (ReqArg (\f c -> c { ifileArg = (f:ifileArg c) }) "FILE") "Input file to check"
            , Option ['d'] ["dht-file"] (ReqArg (\f c -> c {dhtArg = f }) "FILE") "DHT index"
            , Option ['u'] ["uf-file"] (ReqArg (\f c -> c { ufArg = f }) "FILE") "UF File"
            , Option ['v'] ["verbose"] (NoArg (\c -> c {verboseArg = True }))  "Verbose"
            , Option ['t'] ["threads"] (ReqArg (\n c -> c { nJobsArg = read n }) "N") "Nr Threads"
            , Option ['b'] ["blacklist"] (ReqArg (\f c -> c { blackListArg = f }) "N") "Blacklist"
            , Option ['f'] ["full"] (NoArg (\c -> c {fullOutputArg = True })) "Full output"
            ]


ufLookup :: VU.Vector Int -> Int -> Int
ufLookup = (VU.!)

pairsToTriplets :: S.Set B.ByteString -> VU.Vector Int -> DiskHashRO Int64 -> V.Vector B.ByteString -> VS.Vector Word32
pairsToTriplets blacklist uf nameh = toTriplets . parseVector blacklist nameh
    where
        toTriplets v = VS.create $ do
            v' <- VSM.new (VU.length v * 3)
            forM_ [0..VU.length v - 1] $ \ix ->
                let (a, b) = v VU.! ix
                    ai = ufLookup uf a
                    bi = ufLookup uf b
                    in case ai == bi of
                        True -> do
                            VSM.write v' (ix*3 + 0) (toEnum ai)
                            VSM.write v' (ix*3 + 1) (toEnum a)
                            VSM.write v' (ix*3 + 2) (toEnum b)
                        False -> error ( "Mismatched pair (" ++ show ix ++"): "++show a++"-> "++show ai ++" /= "++show b ++ "-> "++show bi)
            return v'


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
    blacklist <- S.fromList <$> C.runConduitRes (CB.sourceFile (blackListArg opts) .| CB.lines .| CC.sinkList)
    when (verboseArg opts) $
        putStrLn $ "Loaded black list: " ++ show (S.size blacklist) ++ " entries."

    uf <- loadUf ngenes (ufArg opts)
    when (verboseArg opts) $
        putStrLn "Loaded union find structure"
    withOutputFile (ofileArg opts) $ \hout ->
        forM_ (ifileArg opts) $ \ifile -> do
            when (verboseArg opts) $
                putStrLn ("Processing file '"++ifile++"'")
            C.runConduitRes $
                conduitPossiblyCompressedFile ifile
                    .| CB.lines
                    .| CC.conduitVector 8192
                    .| asyncMapC nthreads (pairsToTriplets blacklist uf nameh)
                    .| writeWord32VS
                    .| CB.sinkHandle hout
    when (verboseArg opts) $
        putStrLn "Done."

