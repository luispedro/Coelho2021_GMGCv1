{-# LANGUAGE OverloadedStrings #-}

import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as B
import qualified Data.ByteString.Builder as BB
import qualified Data.Vector as V
import qualified Data.Set as S
import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.Combinators as CC
import           Data.Conduit ((.|))
import           Control.Exception (assert)
import qualified Control.Concurrent.Async as A
import qualified Data.Conduit.TQueue as CT
import           Control.Concurrent (setNumCapabilities)
import           System.Console.GetOpt
import           System.Environment (getArgs)
import           Data.List (foldl')
import           Data.Monoid
import           Control.Monad.IO.Class
import           System.IO.SafeWrite

import Control.Concurrent.STM.TBMQueue
import Data.Conduit.Algorithms.Async
import Control.Monad.STM

data Triplet = Triplet
                { tpLeft :: !B.ByteString
                , tpRel :: !Char
                , tpRight :: !B.ByteString
                } deriving (Eq, Ord, Show)

cartProduct :: [a] -> [b] -> [(a, b)]
cartProduct [] _ = []
cartProduct (x:xs) ys = [(x,y) | y <- ys] ++ cartProduct xs ys

listNub :: Ord a => [a] -> [a]
listNub = S.toList . S.fromList

build2steps :: [Triplet] -> [Triplet] -> [Triplet]
build2steps xs ys =
    let build1 (Triplet a rel b) (Triplet a' rel' b') = assert (b == a') $ Triplet a (buildrel rel rel') b'
        buildrel :: Char -> Char -> Char
        buildrel r0 r1
            | r0 `notElem` ("C=" :: [Char]) = error ("BUG: r0="++show r0)
            | r1 == 'R' = 'R'
            | r1 == 'C' = 'C'
            | r1 == '=' = r0
            | otherwise = error ("Should not have happened '"++show r0++"' / '" ++ show r1)
    in listNub $ map (uncurry build1) $ cartProduct xs ys


joinStreams :: TBMQueue [[Triplet]] -> TBMQueue [[Triplet]] -> TBMQueue ([Triplet], [Triplet]) -> IO ()
joinStreams lefts rights out = do
        ell <- read1OrExhaust lefts rights
        r <- read1OrExhaust rights lefts
        go ell r
    where
        read1OrExhaust active other = do
            active' <- read1 active
            case active' of
                Just val -> return val
                Nothing -> do
                    exhaust other
                    atomically $ closeTBMQueue out
                    return []
        read1 = atomically . readTBMQueue
        key1 = tpRight . head
        key2 = tpLeft . head
        exhaust chan = read1 chan >>= \case
            Nothing -> return ()
            Just{} -> exhaust chan

        go :: [[Triplet]] -> [[Triplet]] -> IO ()
        go [] r = do
            ell' <- read1OrExhaust lefts rights
            if null ell'
                then return ()
                else go ell' r
        go ell [] = do
            r' <- read1OrExhaust rights lefts
            if null r'
                then return ()
                else go ell r'
        go ell@(left:left') r@(right:right') = case compare (key1 left) (key2 right) of
            EQ -> do
                atomically $ writeTBMQueue out (left,right)
                go left' right'
            LT -> go left' r
            GT -> go ell right'

parseTrips :: Monad m => C.Conduit B.ByteString m Triplet
parseTrips = CB.lines .| CL.map toTrips
    where
        toTrips line = case B8.split '\t' line of
                        [a,b,c] -> Triplet a (B8.head b) c
                        _ -> error $ "Could not parse triplet: " ++ show line

tab = BB.char7 '\t'
nl = BB.char7 '\n'
encodeTrips :: Triplet -> BB.Builder
encodeTrips (Triplet a b c) = BB.byteString a <> tab <> BB.char7 b <> tab <> BB.byteString c <> nl

data CmdArgs = CmdArgs
    { ifile1Arg :: FilePath
    , ifile2Arg :: FilePath
    , ofileArg :: FilePath
    , verboseArg :: Bool
    , nJobsArg :: Int
    } deriving (Show)


parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' (flip ($)) (CmdArgs "" "" "" False 1) flags
    where
        flags :: [CmdArgs -> CmdArgs]
        (flags, _args, _extraOpts) = getOpt Permute options argv
        options =
            [ Option ['o'] ["output"] (ReqArg (\f c -> c { ofileArg = f }) "FILE") "Output file"
            , Option ['1'] ["input1"] (ReqArg (\f c -> c { ifile1Arg = f }) "FILE") "Input 1"
            , Option ['2'] ["input2"] (ReqArg (\f c -> c { ifile2Arg = f }) "FILE") "Input 2"
            , Option ['v'] ["verbose"] (NoArg (\c -> c {verboseArg = True }))  "Verbose"
            , Option ['t'] ["threads"] (ReqArg (\n c -> c { nJobsArg = read n }) "N") "Nr Threads"
            ]
main :: IO ()
main = do
    opts <- parseArgs <$> getArgs
    let nthreads = nJobsArg opts
    setNumCapabilities nthreads
    lefts <- newTBMQueueIO 8
    rights <- newTBMQueueIO 8
    combined <- newTBMQueueIO 8
    let readLeft = C.runConduitRes $
            CC.sourceFile (ifile1Arg opts)
                .| parseTrips
                .| CL.groupOn1 tpRight
                .| CL.map (uncurry (:))
                .| CC.conduitVector 64
                .| CL.map V.toList
                .| CT.sinkTBMQueue lefts True
        readRight = C.runConduitRes $
            CC.sourceFile (ifile2Arg opts)
                .| parseTrips
                .| CL.groupOn1 tpLeft
                .| CL.map (uncurry (:))
                .| CC.conduitVector 64
                .| CL.map V.toList
                .| CT.sinkTBMQueue rights True
        joinAct = joinStreams lefts rights combined
        writeOut = withOutputFile (ofileArg opts) $ \hout ->
            C.runConduitRes $
                CT.sourceTBMQueue combined
                    .| CC.conduitVector 32
                    .| asyncMapC nthreads (BB.toLazyByteString . mconcat . V.toList . V.map (mconcat . map encodeTrips . uncurry build2steps))
                    .| CL.mapM_ (\block -> liftIO $ BL.hPut hout block)
    readLeft
        `A.concurrently_` readRight
        `A.concurrently_` joinAct
        `A.concurrently_` writeOut

