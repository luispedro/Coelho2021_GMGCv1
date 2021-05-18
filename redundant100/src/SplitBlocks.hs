#!/usr/bin/env stack
-- stack --resolver lts-9.4 script
{-# LANGUAGE LambdaCase, OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables, FlexibleContexts #-}
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit as C
import           Data.Conduit ((.|))
import           Safe (atDef)
import Data.Conduit.Algorithms.Utils (awaitJust)
import Control.Monad
import System.Environment
import System.Console.GetOpt
import Data.Maybe
import Data.List

import Data.BioConduit

data IsolationMode = NSeqs Int | NBPs Int
                        deriving (Eq, Show)

splitFiles :: FilePath -> FilePath -> IsolationMode -> IO ()
splitFiles ifile ofileBase block = C.runConduitRes $
        CB.sourceFile ifile
            .| faConduit
            .| splitWriter
    where
        splitWriter = splitWriter' (0 :: Int)
        splitWriter' n = do
            isolator block
                .| faWriteC
                .| CB.sinkFileCautious (ofileBase ++ "." ++ show n ++ ".fna")
            whenM hasMore $
                splitWriter' $! n + 1
        hasMore = isJust <$> CC.peek
        isolator (NSeqs nSeqs) = CL.isolate nSeqs
        isolator (NBPs nBps) = getNbps nBps
        getNbps n
            | n <= 0 = return ()
            | otherwise = awaitJust $ \fa -> do
                                        C.yield fa
                                        getNbps (n - faseqLength fa)
        whenM cond act = do
            cval <- cond
            if cval
                then act
                else return ()

data CmdArgs = CmdArgs
                { ifileArg  :: FilePath
                , ofileArg :: FilePath
                , nSeqsArg :: Maybe Int
                , nBpsArg :: Maybe Int
                } deriving (Show)


data CmdFlags = OutputFile FilePath
                | InputFile FilePath
                | NSeqFlag Int
                | NBPsFlag Int
                deriving (Eq, Show)

parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' p (CmdArgs ifile ofile Nothing Nothing) flags
    where
        (flags, args, _extraArgs) = getOpt Permute options argv
        ifile = atDef "" args 0
        ofile = atDef "" args 1
        options =
            [ Option ['o'] ["output"]  (ReqArg OutputFile "FILE") "Output file"
            , Option ['i'] ["input"]   (ReqArg InputFile "FILE") "Input file to check"
            , Option ['n'] ["nr-seqs"] (ReqArg (NSeqFlag . read) "INT") "Nr of sequences per block"
            , Option ['b'] ["nr-bps"]  (ReqArg (NBPsFlag . read) "INT") "Nr of base pairs per block"
            ]
        p c (OutputFile f) = c { ofileArg = f }
        p c (InputFile f) = c { ifileArg = f }
        p c (NSeqFlag n) = c { nSeqsArg = Just n }
        p c (NBPsFlag n) = c { nBpsArg = Just n }

main :: IO ()
main = do
    CmdArgs ifile ofileBase nrSeqs nrBps <- parseArgs <$> getArgs
    isolationMode <- case (nrSeqs, nrBps) of
        (Just _, Just _) -> do
            error "Cannot use both nr-seqs and nr-bps arguments"
        (Nothing, Nothing) -> do
            error "Must use either both nr-seqs or nr-bps arguments"
        (Just n, Nothing) -> return $ NSeqs n
        (Nothing, Just b) -> return $ NBPs b
    splitFiles ifile ofileBase isolationMode
