#!/usr/bin/env stack
-- stack --resolver lts-9.4 script
{-# LANGUAGE LambdaCase, OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables, FlexibleContexts #-}
import qualified Data.ByteString.Char8 as B8
import qualified Data.Conduit.Combinators as C
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit as C
import           Data.Conduit ((.|))
import           Safe (atDef)
import           System.IO.SafeWrite (withOutputFile)
import           System.Environment
import System.Console.GetOpt
import Data.List
import Control.Monad.IO.Class
import System.IO
import Data.Conduit.SafeWrite (atomicConduitUseFile)

import Data.BioConduit
import Data.Conduit.Algorithms.Utils

printEvery10k :: (MonadIO m) => C.Conduit a m a
printEvery10k = printEvery10k' (1 :: Int) (10000 :: Int)
    where
        printEvery10k' n 0 = do
            liftIO $ putStrLn ("Read "++show (n *10) ++"k reads")
            printEvery10k' (n+1) 10000
        printEvery10k' n r = C.await >>= \case
            Nothing -> return ()
            Just v -> do
                C.yield v
                printEvery10k' n (r - 1)


renameTo renametable = enumerate .| renameTo'
    where
        enumerate = enumerate' (0 :: Int)
        enumerate' n = awaitJust $ \v -> do
                        C.yield (n, v)
                        enumerate' $ n + 1
        renameTo' = atomicConduitUseFile renametable $ \h ->
                        C.awaitForever $ \(n, fa) -> do
                            let nname = "Fr12_" ++ show n
                                fa' = Fasta (B8.pack nname) (seqdata fa)
                            C.yield fa'
                            liftIO $ hPutStrLn h (nname ++ "\t" ++ B8.unpack (seqheader fa))

data CmdFlags = OutputFile FilePath
                | InputFile FilePath
                | RenameFile FilePath
                | NJobs Int
                | Verbose
                deriving (Eq, Show)

options :: [OptDescr CmdFlags]
options =
    [ Option ['v'] ["verbose"] (NoArg Verbose)  "verbose mode"
    , Option ['i'] ["input"] (ReqArg InputFile "FILE") "Input file to check"
    , Option ['o'] ["output"] (ReqArg OutputFile "FILE") "Output file"
    , Option ['r'] ["output"] (ReqArg RenameFile "FILE") "Renamed file output"
    , Option ['j'] ["threads", "jobs"] (ReqArg (NJobs . read) "INT") "Nr threads"
    ]


data CmdArgs = CmdArgs
                    { ifile :: FilePath
                    , ofile :: FilePath
                    , rfile :: FilePath
                    , verbose :: Bool
                    , nJobs :: Int
                    } deriving (Eq, Show)

parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' p (CmdArgs iarg oarg rarg False 8) flags
    where
        (flags, args, _extraOpts) = getOpt Permute options argv
        iarg = atDef "" args 0
        oarg = atDef "" args 1
        rarg = atDef "" args 2
        p c Verbose = c { verbose = True }
        p c (OutputFile o) = c { ofile = o }
        p c (RenameFile o) = c { rfile = o }
        p c (InputFile i) = c { ifile = i }
        p c (NJobs n) = c { nJobs = n }

main :: IO ()
main = do
    opts <- parseArgs <$> getArgs
    print opts
    C.runConduitRes $
        C.sourceFile (ifile opts)
        .| faConduit
        .| printEvery10k
        .| renameTo (rfile opts)
        .| faWriteC
        .| CB.sinkFileCautious (ofile opts)
