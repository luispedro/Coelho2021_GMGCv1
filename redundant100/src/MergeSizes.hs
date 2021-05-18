import qualified Data.ByteString.Char8 as B8
import qualified Data.Conduit as C
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
import Data.Conduit.Algorithms
import Data.BioConduit
import Algorithms.OutSort

newtype FASize = FASize { unwrapFASize :: Fasta }

instance Eq FASize where
    (FASize a) == (FASize b) = (a == b)

instance Ord FASize where
    compare = comparing $ \(FASize a) -> (faseqLength a, seqdata a, seqheader a)


mergeSizes :: [FilePath] -> FilePath -> IO ()
mergeSizes ifiles ofile = C.runConduitRes $
    mergeC [(CB.sourceFile f .| faConduit .| CL.map FASize) | f <- ifiles]
        .| CL.map unwrapFASize
        .| faWriteC
        .| CB.sinkFileCautious ofile


data CmdArgs = CmdArgs
                { argIfile :: FilePath
                , argOfile :: FilePath
                , argIFileList :: FilePath
                , nJobs :: Int
                } deriving (Eq, Show)

data CmdFlags = OutputFile FilePath
                | InputFile FilePath
                | ListFile FilePath
                | NJobs Int
                | Verbose
                deriving (Eq, Show)

options :: [OptDescr CmdFlags]
options =
    [ Option ['v'] ["verbose"] (NoArg Verbose)  "verbose mode"
    , Option ['i'] ["input"] (ReqArg InputFile "FILE") "Input file"
    , Option ['F'] ["file-list"] (ReqArg ListFile "FILE") "Input is a list of files"
    , Option ['o'] ["output"] (ReqArg OutputFile "FILE") "Output file"
    , Option ['j'] ["threads", "jobs"] (ReqArg (NJobs . read) "INT") "Nr threads"
    ]


parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' p (CmdArgs ifile ofile "" 1) flags
    where
        (flags, args, _extraOpts) = getOpt Permute options argv
        ifile = atDef "" args 0
        ofile = atDef "" args 1

        p c (OutputFile o) = c { argOfile = o }
        p c (InputFile i) = c { argIfile = i }
        p c (ListFile f) = c { argIFileList = f }
        p c (NJobs n) = c { nJobs = n }
        p c Verbose = c

main :: IO ()
main = do
    opts <- parseArgs <$> getArgs
    let nthreads = nJobs opts
    setNumCapabilities nthreads
    ifiles <- case (argIfile opts, argIFileList opts) of
            (ifile, "") -> return [ifile]
            ("", ffile) -> C.runConduitRes $
                                CB.sourceFile ffile
                                    .| CB.lines
                                    .| CL.map B8.unpack
                                    .| CL.consume
            _ -> do
                putStrLn "Cannot pass both input file and -F argument"
                exitFailure
    mergeSizes ifiles (argOfile opts)
