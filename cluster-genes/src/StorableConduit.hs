{-# LANGUAGE LambdaCase, OverloadedStrings, FlexibleContexts, BangPatterns, PackageImports #-}
module StorableConduit
    ( writeWord32VS
    , readWord32VS
    ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as BU
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Storable.Mutable as VSM
import Control.Monad.IO.Class

import Data.Word
import Foreign.Ptr
import Foreign.Marshal.Utils
import Foreign.Storable

import qualified Data.Conduit.List as CL
import qualified Data.Conduit as C
import           Data.Conduit ((.|))
import qualified Data.Conduit.Algorithms.Utils as CAlg
import qualified Data.Conduit.Algorithms.Storable as CS

writeWord32VS :: (MonadIO m, Monad m) => C.Conduit (VS.Vector Word32) m B.ByteString
writeWord32VS = CS.writeStorableV

readWord32VS :: MonadIO m => Int -> C.ConduitM B.ByteString (VS.Vector Word32) m ()
readWord32VS = CS.readStorableV
