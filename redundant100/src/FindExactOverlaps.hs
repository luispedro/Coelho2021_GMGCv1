#!/usr/bin/env stack
-- stack --resolver lts-8.13 script
{-# LANGUAGE LambdaCase, OverloadedStrings, BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables, FlexibleContexts #-}
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Search as BSearch
import qualified Data.Conduit.Combinators as C
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as C
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit as C
import           Data.Conduit ((.|))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified Data.IntMap.Strict as IM
import           Safe (headDef, atDef)
import           System.IO.SafeWrite (withOutputFile)
import           Data.Strict.Tuple (Pair(..))
import           Control.Concurrent (setNumCapabilities)
import System.Environment
import           Control.Monad.Extra (whenJust)
import           Control.Applicative ((<|>))
import Control.Monad
import System.Console.GetOpt
import Data.Maybe
import Data.Bits
import Data.List
import Control.Monad.IO.Class
import Control.Monad.Base
import Data.Word
import qualified Data.HashTable.IO as HT

import Data.BioConduit
import Data.Conduit.Algorithms.Utils
import Data.Conduit.Algorithms.Async

-- This is the same as Data.Hash.combine, but we cannot use it because it does
-- not export the Hash constructor
hashCombine :: Word64 -> Word64 -> Word64
hashCombine !a !b = rotate a 1 `xor` b
{-# INLINE hashCombine #-}

hash8v :: VU.Vector Word64
hash8v = VU.fromList [
 9500514830699277030, 14478430058364912793, 5475998190937965401, 7624200594592030974, 15009460848216803030, 6389105156167320271,
 18441117592457556219, 15642107930188812694, 12740178482041212977, 15684538367533127958, 8926426149937545710, 2338396044873990761,
 14771902185307135797, 13264766869694282515, 6045519973703685355, 3083948859584282496, 1457559084049579671, 5930189240902606857,
 870519245627253250, 4462052888688229454, 191815745574903993, 11903414619901391455, 11727812422944757080, 1269406778347126253,
 15998855849663127830, 8393838932765003498, 14762962132528121810, 18219895165183247326, 16533303768288047797, 85097248796741531,
 410277522747002276, 17928095439243872671, 15622356009269801052, 7169388131474323899, 16940865941294561738, 14237175016037736734,
 17827926089700543940, 13394333370879980210, 8365474295309962641, 7195194656684355717, 6420704835050947958, 3882595584769580796,
 6274938662942099642, 9458989032881914985, 7880412838516310573, 7546046290046923985, 11430819016910127831, 2625192554458255301,
 1962139780852365426, 14436484115471517676, 1899590106343018035, 6029017667087537934, 6074321841245937458, 17904958046105460756,
 12540328364125410285, 11081655182743446725, 14901534198199156731, 11322322183866435649, 18406614367193509774, 3360167739402781549,
 925361933624796548, 15237385593036964626, 12265913749666666610, 5080459173836116447, 8078074027379977720, 6911806877851466053,
 4642990625734696547, 15398823370945195303, 7866211193936328770, 11818210170543189683, 1001886154452850075, 6292769360879455103,
 13532284662138770296, 16524791927921440899, 16317138762070759128, 5245156148315724158, 3097748128826464437, 7244015920825117125,
 11286739005130425738, 6962004855520570371, 10253245250890538492, 6847564388132021726, 14082786902126823205, 5793990675770984038,
 7027117957116303596, 13116872285497922729, 14109110319109965700, 13823322679274856858, 13069867890039363840, 147942074856336100,
 5245339324645023356, 12140942771732569528, 6907401848172852737, 14357698522908149805, 16787312984472793331, 6388527199101643220,
 10291329600600348099, 4282391516385830059, 14786685944771058405, 10702790283523343223, 8693432648425811849, 14164384572113824741,
 8529572394667220568, 13360249940784187743, 7666652751264705772, 232648290071156928, 1563130283042714463, 7441678105307404078,
 9643111585517183405, 10731130690684784441, 2093356038204611246, 2215462090274908186, 2893181046657629146, 3773555486441642087,
 2298589703496497424, 4708765815642767920, 4587584140152198695, 15499645202918514493, 8390102628668347132, 18110499191208973006,
 2633284928256471186, 12848535874111893296, 2422011268628645742, 2818993246680203275, 1826625082824332109, 18100080556869136616,
 1226328544818089633, 8385870463680778347, 11071641508705418268, 1587422367830252719, 7696713076100367262, 6105831358054263977,
 929329693671641246, 15641035492054849779, 12063656172135103766, 7599096217785353011, 3671076276680339896, 11461131025805838997,
 12651348006325924540, 9313085477043301931, 11436608907505903890, 499876177625798718, 8772236708288828316, 12202678818225342015,
 3055510532846225706, 8518529862577512831, 16588883255681132563, 16692267770707729856, 14441755696608686174, 12863553652539895920,
 9579641677361790295, 17076685085105159968, 6993749144297397373, 5119635826259591586, 11728389174804082600, 15931714724494098231,
 8358833071956779015, 11665986513756949469, 6032389331024009400, 17688491799502138491, 10962564482897076612, 1209179395836295124,
 11934095944956138704, 16522618356194473146, 12560860520674576649, 10025260171884867415, 521139671789152458, 2272407426670456811,
 5699415091704243224, 8900214064280321596, 4968601286103132944, 6974407590623971293, 4388922621761228934, 500576111568551964,
 3252771761011972699, 9739261630835269035, 10376447719058200659, 12015093549766946107, 5002819766980710372, 4576626500488409632,
 6145863787462695426, 17892667896400767238, 17905731519165087714, 12473058868733933486, 3715635057894518929, 8748777864909075310,
 10051451801855683909, 1469318722920344171, 1474618170327597483, 16851108282887418799, 15614531836774421486, 7120524625260931933,
 692606843046927590, 12043736386941474243, 6968626744858556008, 11707004989346247625, 9779391867963964823, 14723102455402089566,
 11381570846382797829, 15334257992951337635, 526930684396893818, 7362144726834218559, 16246781423819608350, 6934619055339027748,
 1747120681392596315, 13725099895669661101, 2597748241528058876, 6838638887852635366, 14464392985216828846, 15633034287276951431,
 17933003326157541046, 3979758093987441028, 16636027463187911318, 4968892912584085039, 18074832298958267576, 1180290621018036665,
 9466286528482500605, 7667556245236151965, 11546037644998404100, 3288784030840230834, 13057504296680298945, 8214019483773522583,
 5298218069071078526, 9446081649308885777, 1163990934617599132, 7446139900602031581, 17749395101427129083, 4681632553539739213,
 6735947831871608848, 14883867459772936768, 8827773120446508810, 10066213943156522199, 10256118081766969598, 4120692917607911124,
 15121474290497544280, 9401016560453584572, 11284845072667533955, 5919655777125939366, 4366706624682943775, 16792774723158826807,
 2848760740139833529, 16535302204321626304, 16846020875485347342, 9085261194011343348, 2313567038150627704, 171586936636335814,
 1436085244137652348, 9841677663128434517, 13286645925681549782, 15394481243391698604, 2925230850336533286, 3842174860857429961,
 406607388048326633, 5809996587542008338, 17131939028413721386, 2771536858083382320]

hash8 :: Word8 -> Word64
hash8 !v = hash8v `VU.unsafeIndex` (fromEnum v)
{-# INLINE hash8 #-}

hash0 :: Word64
hash0 = 17343750869553076467

rollall :: Int -> B.ByteString -> VU.Vector Int
rollall n bs
    | n > B.length bs = VU.empty
    | otherwise = VU.create $ do
        res <- VUM.new (B.length bs - n + 1)
        let inithash !ch p
                | p == n - 1 = ch :: Word64
                | otherwise = inithash (ch `hashCombine` hash8 (B.index bs p)) (p+1)
            iter !ch !p !pix
                | p == B.length bs = return res
                | otherwise =  do
                    let ck = hash8 (B.index bs p)
                        cl
                           | pix == 0 = hash0
                           | otherwise = hash8 (B.index bs (pix-1))
                        ch' :: Word64
                        ch' = ch `hashCombine` (rotate cl n `xor` ck)
                    VUM.unsafeWrite res (p - n + 1) (fromIntegral ch')
                    iter ch' (p+1) (if pix + 1 == n then 0 else pix + 1)
        iter (inithash hash0 0) (n-1) 0


type FastaIOHash = HT.CuckooHashTable Int [Fasta]
type FastaMap = IM.IntMap [Fasta -> Maybe B.ByteString]
data SizedHash = SizedHash !Int !FastaMap

faContainedIn :: Fasta -> Fasta -> Bool
faContainedIn (Fasta _ da) (Fasta _ db) = not . null $ BSearch.indices da db


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

annotate :: Int -> Fasta -> (Fasta, (VU.Vector Int))
annotate hashSize fa@(Fasta _ fad) = (fa, rollall hashSize fad)

buildHash :: (MonadIO m, MonadBase IO m) => Int -> C.Sink Fasta m SizedHash
buildHash nthreads = C.peek >>= \case
        Nothing -> error "Empty stream"
        Just first -> do
            let hashSize = faseqLength first
            SizedHash hashSize <$>
                    (C.conduitVector 4096
                    .| asyncMapC (2 * nthreads) (V.map (annotate hashSize))
                    .| CL.fold insertmany IM.empty)
    where
        matcher (Fasta h s) = let
             test = B.isInfixOf s
           in \(Fasta _ seq') -> if test seq' then Just h else Nothing
        insertmany imap = V.foldl addHash imap
        addHash imap (faseq, hashes) = IM.alter (Just . (matcher faseq:) . (fromMaybe [])) curk imap
            where
                hashes' = VU.toList hashes
                curk = headDef (head hashes') (filter (flip IM.notMember imap) hashes')

findOverlapsSingle nthreads = do
    first <- C.peek
    h <- liftIO HT.new
    whenJust first $ \faseq ->
        CC.conduitVector 4096
            .| asyncMapC nthreads (V.map (annotate (faseqLength faseq)))
            .| CC.concat
            .| findOverlapsSingle' h
  where
    findOverlapsSingle' :: MonadIO m => FastaIOHash -> C.Conduit (Fasta, VU.Vector Int) m B.ByteString
    findOverlapsSingle' imap = awaitJust $ \(faseq@(Fasta sid _), hashes) -> do
        let lookup1 :: Pair [Fasta] (Maybe Int) -> Int -> IO (Pair [Fasta] (Maybe Int))
            lookup1 (ccovered :!: mcurk) h = do
                val <- HT.lookup imap h
                return $! case val of
                    Nothing -> (ccovered :!: mcurk <|> Just h)
                    Just prevs -> (filter (`faContainedIn` faseq) prevs ++ ccovered :!: mcurk)
        (covered :!: minsertpos) <- liftIO $ VU.foldM lookup1 ([] :!: Nothing) hashes
        -- If we failed to find an empty slot for the sequence, just use first one
        let insertpos = fromMaybe (VU.head hashes) minsertpos

        forM_ covered $ \c ->
            C.yield (B.concat [seqheader c, "\tC\t", sid])
        liftIO $ HT.mutate imap insertpos (\curv -> (Just . (faseq:) . fromMaybe [] $ curv, ()))
        findOverlapsSingle' imap

findOverlapsAcross :: (MonadIO m, MonadBase IO m) => Int -> SizedHash -> C.Conduit Fasta m B.ByteString
findOverlapsAcross n (SizedHash hashSize imap) = CC.conduitVector 4096 .| asyncMapC (2 * n) findOverlapsAcross'
    where
        findOverlapsAcross' :: V.Vector Fasta -> B.ByteString
        findOverlapsAcross' = B.concat . concatMap findOverlapsAcross'1
        findOverlapsAcross'1 :: Fasta -> [B.ByteString]
        findOverlapsAcross'1 faseq@(Fasta sid _) = concat [[c, "\tC\t", sid, "\n"] | c <- covered]
            where
                -- Starting in containers 0.5.8, we could use the
                -- IM.restrictKeys function to "lookup" all the keys at once
                hashes = rollall hashSize (seqdata faseq)
                candidates = flip concatMap (VU.toList hashes) $ \h -> IM.findWithDefault [] h imap
                covered = mapMaybe ($ faseq) candidates

data CmdFlags = OutputFile FilePath
                | InputFile FilePath
                | FilesFromFile FilePath
                | NJobs Int
                | Verbose
                | Mode2Flag
                deriving (Eq, Show)

options :: [OptDescr CmdFlags]
options =
    [ Option ['v'] ["verbose"] (NoArg Verbose)  "verbose mode"
    , Option ['2'] [] (NoArg Mode2Flag)         "2 file mode"
    , Option ['i'] ["input"] (ReqArg InputFile "FILE") "Input file to check"
    , Option ['o'] ["output"] (ReqArg OutputFile "FILE") "Output file"
    , Option ['e'] ["extra-list"] (ReqArg FilesFromFile "FILE") "Input files to check against"
    , Option ['j'] ["threads", "jobs"] (ReqArg (NJobs . read) "INT") "Nr threads"
    ]

data Mode = ModeSingle | ModeAcross
    deriving (Eq,Show)

data CmdArgs = CmdArgs
                    { ifile :: FilePath
                    , ofile :: FilePath
                    , mode :: Mode
                    , verbose :: Bool
                    , nJobs :: Int
                    , extraFilesFilesFrom :: FilePath
                    } deriving (Eq, Show)

parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' p (CmdArgs iarg oarg ModeSingle False 8 []) flags
    where
        (flags, args, _extraOpts) = getOpt Permute options argv
        iarg = atDef "" args 0
        oarg = atDef "" args 1
        p c Verbose = c { verbose = True }
        p c (OutputFile o) = c { ofile = o }
        p c (InputFile i) = c { ifile = i }
        p c (FilesFromFile i) = c { extraFilesFilesFrom = i }
        p c (NJobs n) = c { nJobs = n }
        p c Mode2Flag = c { mode = ModeAcross }

main :: IO ()
main = do
    opts <- parseArgs <$> getArgs
    print opts
    let nthreads = nJobs opts
    setNumCapabilities nthreads
    case mode opts of
        ModeSingle -> C.runConduitRes $
            C.sourceFile (ifile opts)
                .| faConduit
                .| findOverlapsSingle nthreads
                .| C.unlinesAscii
                .| CB.sinkFileCautious (ofile opts)
        ModeAcross -> do
            h <- C.runConduitRes $
                C.sourceFile (ifile opts)
                    .| faConduit
                    .| buildHash nthreads
            putStrLn "Built initial hash"
            extraFiles <-
                C.runConduitRes $
                    C.sourceFile (extraFilesFilesFrom opts)
                    .| C.lines
                    .| CL.map B8.unpack
                    .| CL.consume
            withOutputFile (ofile opts) $ \hout ->
                forM_ extraFiles $ \fafile -> do
                    putStrLn ("Handling file "++fafile)
                    C.runConduitRes $
                        C.sourceFile fafile
                        .| faConduit
                        .| findOverlapsAcross nthreads h
                        .| C.sinkHandle hout
            putStrLn "Done."
