{-# LANGUAGE LambdaCase, OverloadedStrings, FlexibleContexts, BangPatterns, PackageImports #-}
module PairParsing
    ( parseVector
    ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified Data.Set as S

import Data.Strict.Tuple
import Data.Int
import Data.DiskHash

parseMatches :: B.ByteString -> Pair B.ByteString B.ByteString
parseMatches line = case B8.split '\t' line of
    [a, r, b]
        | r == "R" -> a :!: b
    _ -> error $ "Could not parse line: '"++B8.unpack line ++"'"

lookupMatches :: DiskHashRO Int64 -> Pair B.ByteString B.ByteString -> Pair Int Int
lookupMatches nameh (n0 :!: n1) = lookup' n0 :!: lookup' n1
    where
        lookup' n = case htLookupRO n nameh of
                        Just v -> fromEnum v
                        Nothing -> error $ "Name not in hash table: '"++B8.unpack n++"' ("++(B8.unpack n)++")."

parseVector :: S.Set B.ByteString -> DiskHashRO Int64 -> V.Vector B.ByteString -> VU.Vector (Int, Int)
parseVector blacklist nameh v = VU.create $ do
                            v' <- VUM.new (V.length v)
                            let add outIx inIx
                                    | inIx == V.length v = return outIx
                                    | otherwise = do
                                        let sa :!: sb = parseMatches (v V.! inIx)
                                        if sa `S.member` blacklist || sb `S.member` blacklist
                                            then add outIx (inIx + 1)
                                            else do
                                                let a :!: b = lookupMatches nameh (sa :!: sb)
                                                VUM.write v' outIx (a,b)
                                                add (outIx + 1) (inIx + 1)
                            s <- add 0 0
                            return $ VUM.unsafeSlice 0 s v'



