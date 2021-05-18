#!/usr/bin/env stack
-- stack script --resolver lts-8.13 --optimize
{-# LANGUAGE LambdaCase, OverloadedStrings, FlexibleContexts, BangPatterns, PackageImports #-}

import qualified Data.ByteString as B
import qualified Data.Vector.Storable as VS
import           Control.Concurrent (setNumCapabilities)
import           System.Console.GetOpt
import           System.Environment (getArgs)
import           Data.List (foldl')
import Control.Monad
import Control.Monad.IO.Class

import Data.Word
import Debug.Trace

import qualified Data.Conduit as C
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import           Data.Conduit ((.|))
import qualified Data.Conduit.Algorithms as CAlg

import StorableConduit (writeWord32VS, readWord32VS)

tracex :: Show a => a -> a
tracex x = trace (show x) x

data Trip32 = Trip32
                    {-# UNPACK #-} !Word32
                    {-# UNPACK #-} !Word32
                    {-# UNPACK #-} !Word32
                deriving (Eq, Show)

instance Ord Trip32 where
    compare (Trip32 a0 a1 a2) (Trip32 b0 b1 b2)
        | a0 /= b0 = compare a0 b0
        | a1 /= b1 = compare a1 b1
        | otherwise = compare a2 b2


data CmdArgs = CmdArgs
        { ifilesArg :: [FilePath]
        , ofileArg :: FilePath
        , nJobsArg :: Int
        , verboseArg :: Bool
        } deriving (Show)


parseTrips :: MonadIO m => C.Conduit B.ByteString m Trip32
parseTrips = readWord32VS (3 * 2048) .| by3s
    where

        by3s = C.awaitForever $ \v -> do
                    if VS.length v `mod` 3 /= 0
                        then error "NOT A MULTIPLE OF 3"
                        else return ()
                    forM_ [0 .. (VS.length v `div` 3) - 1] $ \ix -> do
                        let a = v VS.! (ix * 3 + 0)
                            b = v VS.! (ix * 3 + 1)
                            c = v VS.! (ix * 3 + 2)
                        C.yield $! Trip32 a b c


encodeTrips = g3s .| CC.conduitVector 8192 .| writeWord32VS
    where
        g3s = (C.awaitForever $ \(Trip32 a b c) -> C.yield a >> C.yield b >> C.yield c)

parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' (flip ($)) (CmdArgs [] "" 1 False) flags
    where
        flags :: [CmdArgs -> CmdArgs]
        (flags, _, _extraOpts) = getOpt Permute options argv
        options =
            [ Option ['o'] ["output"] (ReqArg (\f c -> c { ofileArg = f }) "FILE") "Output file"
            , Option ['i'] ["input"] (ReqArg (\f c -> c { ifilesArg = (f:ifilesArg c) }) "FILE") "Input file"
            , Option ['v'] ["verbose"] (NoArg (\c -> c {verboseArg = True }))  "Verbose"
            , Option ['t'] ["threads"] (ReqArg (\n c -> c { nJobsArg = read n }) "N") "Nr Threads"
            ]



main :: IO ()
main = do
    opts <- parseArgs <$> getArgs
    print opts
    let nthreads = nJobsArg opts
    setNumCapabilities nthreads
    let inputs = [(CB.sourceFile i .| parseTrips) | i <- ifilesArg opts]
    C.runConduitRes $
        CAlg.mergeC inputs
        .| CAlg.removeRepeatsC
        .| CL.filter (\(Trip32 _ v0 v1) -> v0 /= v1)
        .| encodeTrips
        .| CB.sinkFileCautious (ofileArg opts)

