{-# LANGUAGE OverloadedStrings #-}

import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString as B
import qualified Data.Vector as V
import qualified Data.Set as S
import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.Combinators as CC
import           Data.Conduit ((.|))
import           Control.Concurrent (setNumCapabilities)
import           System.Console.GetOpt
import           System.Environment (getArgs)
import           Data.List (foldl')
import Control.Monad
import Data.Conduit.Algorithms.Async (asyncMapC)

isValidTriplet :: S.Set B.ByteString -> Bool -> B.ByteString -> Bool
isValidTriplet valid checkBoth = \line -> case B8.split '\t' line of
                        [a,_,c] -> c `S.member` valid || (checkBoth && a `S.member` valid)
                        _ -> error $ "Could not parse triplet: " ++ show line

data CmdArgs = CmdArgs
    { ifileArg :: FilePath
    , interestFileArg :: FilePath
    , ofileArg :: FilePath
    , bothArg :: Bool
    , verboseArg :: Bool
    , nJobsArg :: Int
    } deriving (Show)


parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' (flip ($)) (CmdArgs "" "" "" False False 1) flags
    where
        flags :: [CmdArgs -> CmdArgs]
        (flags, _args, _extraOpts) = getOpt Permute options argv
        options =
            [ Option ['o'] ["output"] (ReqArg (\f c -> c { ofileArg = f }) "FILE") "Output file"
            , Option ['i'] ["input"] (ReqArg (\f c -> c { ifileArg = f }) "FILE") "Input 1"
            , Option ['c'] ["interest"] (ReqArg (\f c -> c { interestFileArg = f }) "FILE") "Input 2"
            , Option ['2'] ["both"] (NoArg (\c -> c { bothArg = True })) "Check both sides"
            , Option ['v'] ["verbose"] (NoArg (\c -> c {verboseArg = True }))  "Verbose"
            , Option ['t'] ["threads"] (ReqArg (\n c -> c { nJobsArg = read n }) "N") "Nr Threads"
            ]
main :: IO ()
main = do
    opts <- parseArgs <$> getArgs
    let nthreads = nJobsArg opts
    setNumCapabilities nthreads
    valid <- C.runConduitRes $
                    CC.sourceFile (interestFileArg opts)
                    .| CB.lines
                    .| CL.fold (flip S.insert) S.empty
    when (verboseArg opts) $
        putStrLn ("Loaded valid set ("++show (S.size valid)++" entries).")
    C.runConduitRes $
        CC.sourceFile (ifileArg opts)
            .| CB.lines
            .| CC.conduitVector 1024
            .| asyncMapC nthreads (B8.unlines . V.toList . V.filter (isValidTriplet valid (bothArg opts)))
            .| CB.sinkFileCautious (ofileArg opts)

