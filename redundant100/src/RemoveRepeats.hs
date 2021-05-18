#!/usr/bin/env stack
-- stack --resolver lts-9.4 script
{-# LANGUAGE LambdaCase, OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables, FlexibleContexts #-}
import qualified Data.ByteString.Char8 as B8
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit as C
import           Data.Conduit ((.|))
import           Safe (atDef)
import Control.Monad
import System.Environment
import System.Console.GetOpt
import Data.List
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import Data.Conduit.SafeWrite

import Data.BioConduit

filterCopies :: (MonadIO m, MonadResource m) => FilePath -> C.Conduit Fasta m Fasta
filterCopies f = atomicConduitUseFile f (\th -> (CL.groupOn1 seqdata .| yield1 th))
    where
        yield1 th = C.awaitForever $ \(n, rest) -> do
            C.yield n
            forM_ rest $ \r ->
                liftIO $ B8.hPutStrLn th (B8.concat [seqheader r, "\t=\t", seqheader n])

removeRepeats :: FilePath -> FilePath -> FilePath -> IO ()
removeRepeats ifile ofile copies = C.runConduitRes $
    CB.sourceFile ifile
        .| faConduit
        .| filterCopies copies
        .| faWriteC
        .| CB.sinkFileCautious ofile

data CmdArgs = CmdArgs
                { ifileArg  :: FilePath
                , ofileArg :: FilePath
                , duplicatesArg :: FilePath
                , nJobsArg :: Int
                } deriving (Show)


data CmdFlags = OutputFile FilePath
                | InputFile FilePath
                | DuplicatesFile FilePath
                | NJobs Int
                deriving (Eq, Show)


parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' p (CmdArgs ifile ofile dupfile 1) flags
    where
        (flags, args, _extraOpts) = getOpt Permute options argv
        ifile = atDef "" args 0
        ofile = atDef "" args 1
        dupfile = atDef "" args 2
        options =
            [ Option ['o'] ["output"] (ReqArg OutputFile "FILE") "Output file"
            , Option ['d'] ["duplicates"] (ReqArg DuplicatesFile "FILE") "Duplicate file"
            , Option ['i'] ["input"] (ReqArg InputFile "FILE") "Input file to check"
            ]
        p c (OutputFile f) = c { ofileArg = f }
        p c (InputFile f) = c { ifileArg = f }
        p c (DuplicatesFile f) = c { duplicatesArg = f }
        p c (NJobs n) = c { nJobsArg = n }

main :: IO ()
main = do
    CmdArgs ifile ofile duplicates nthreads <- parseArgs <$> getArgs
    removeRepeats ifile ofile duplicates
