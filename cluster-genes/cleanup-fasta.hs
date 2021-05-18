#!/usr/bin/env stack
-- stack script --resolver lts-9.0 --optimize
{-# LANGUAGE LambdaCase, OverloadedStrings, FlexibleContexts, BangPatterns #-}
import qualified Data.ByteString as B
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.Binary
import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL
import           Data.Conduit ((.|))
import System.Environment
import           Data.Conduit.SafeWrite

import Data.BioConduit

main = do
    [ifile, ofile] <- getArgs
    C.runConduitRes $
        CC.sourceFile ifile
            .| faConduit
            .| CL.filter (\s -> B.length (seqdata s) >= 100)
            .| faWriteC
            .| safeSinkFile ofile
