#!/usr/bin/env stack
-- stack --resolver lts-9.0 script --optimize
{-# LANGUAGE LambdaCase, OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables, FlexibleContexts #-}
import qualified Data.ByteString as B
import qualified Data.Conduit.Combinators as C
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.Binary as C
import qualified Data.Conduit as C
import           Data.Conduit ((.|))
import qualified Data.Set as S
import           Data.List (foldl')
import           System.Console.GetOpt
import Control.Monad
import System.Environment


import Data.BioConduit

data CmdArgs = CmdArgs
    { ifilesArg :: [FilePath]
    , keepArg :: FilePath
    , ofileArg :: FilePath
    , verboseArg :: Bool
    } deriving (Show)

parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' (flip ($)) (CmdArgs [] "" "" False) flags
    where
        flags :: [CmdArgs -> CmdArgs]
        (flags, _, _extraOpts) = getOpt Permute options argv
        options =
            [ Option ['o'] ["output"] (ReqArg (\f c -> c { ofileArg = f }) "FILE") "Output file"
            , Option ['k'] ["keep"] (ReqArg (\f c -> c { keepArg = f }) "FILE") "Input file to check"
            , Option ['i'] ["input"] (ReqArg (\f c -> c { ifilesArg = (f:ifilesArg c) }) "FILE") "Input file to check"
            , Option ['v'] ["verbose"] (NoArg (\c -> c {verboseArg = True }))  "Verbose"
            ]


readHeaderList :: FilePath -> IO (S.Set B.ByteString)
readHeaderList cfname =
    C.runConduitRes $
        C.sourceFile cfname
        .| C.lines
        .| CL.fold (flip S.insert) S.empty

main :: IO ()
main = do
    opts <- parseArgs <$> getArgs
    toKeep <- readHeaderList (keepArg opts)
    when (verboseArg opts) $
        putStrLn ("Will keep " ++ show (S.size toKeep) ++ " sequences.")
    C.runConduitRes $
        (sequence_ [C.sourceFile f | f <- ifilesArg opts])
            .| faConduit
            .| C.filter (flip S.member toKeep . seqheader)
            .| faWriteC
            .| CB.sinkFileCautious (ofileArg opts)
