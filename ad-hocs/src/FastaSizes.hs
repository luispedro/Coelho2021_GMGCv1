{-# LANGUAGE OverloadedStrings #-}
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.Conduit as C
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import           Control.Concurrent (setNumCapabilities)
import           Control.Monad.IO.Class (liftIO)
import           Data.Conduit ((.|))
import           System.Console.GetOpt
import           System.Environment (getArgs)
import           Data.Ord (comparing)
import           Data.List (foldl')
import           Safe (atDef)
import           System.Exit (exitFailure)

import Data.BioConduit


data CmdArgs = CmdArgs
                { ifileArg :: FilePath
                , ofileArg :: FilePath
                , verboseArg :: Bool
                , nJobs :: Int
                } deriving (Eq, Show)


parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' (flip ($)) (CmdArgs "" "" False 1) flags
    where
        flags :: [CmdArgs -> CmdArgs]
        (flags, _, _extraOpts) = getOpt Permute options argv
        options =
            [ Option ['o'] ["output"] (ReqArg (\f c -> c { ofileArg = f }) "FILE") "Output file"
            , Option ['i'] ["input"] (ReqArg (\f c -> c { ifileArg = f }) "FILE") "Input file"
            , Option ['v'] ["verbose"] (NoArg (\c -> c {verboseArg = True }))  "Verbose"
            , Option ['j'] ["threads", "jobs"] (ReqArg (\j c -> c { nJobs = read j }) "INT") "Nr threads"
            ]


main :: IO ()
main = do
    opts <- parseArgs <$> getArgs
    let nthreads = nJobs opts
    setNumCapabilities nthreads
    C.runConduitRes $
        CC.sourceFile (ifileArg opts)
            .| faConduit
            .| CL.map (\(Fasta fh fas) -> B.concat [fh,"\t", B8.pack (show $ B.length fas), "\n"])
            .| CB.sinkFileCautious (ofileArg opts)

