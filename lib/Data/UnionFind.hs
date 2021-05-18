module Data.UnionFind
    ( UnionFind(..)
    , new
    , link
    , rep
    , flatten
    ) where

import qualified Data.Vector.Unboxed.Mutable as VUM
import           Control.Monad (forM_, void)

newtype UnionFind = UnionFind (VUM.IOVector Int)

new :: Int -> IO UnionFind
new s = do
    v <- VUM.new s
    forM_ [0..(s-1)] $ \i ->
        VUM.write v i i
    return $! UnionFind v

link :: UnionFind -> Int -> Int -> IO ()
link uf@(UnionFind v) ix0 ix1 = do
    r0 <- rep uf ix0
    r1 <- rep uf ix1
    VUM.write v r0 r1
{-# INLINEABLE link #-}

rep :: UnionFind -> Int -> IO Int
rep uf@(UnionFind v) ix = do
    c <- VUM.read v ix
    if c == ix
        then return ix
        else do
            c' <- rep uf c
            VUM.write v ix c'
            return c'
{-# INLINEABLE rep #-}

flatten :: UnionFind -> IO ()
flatten uf@(UnionFind v) = forM_ [0..VUM.length v - 1] $ void . (rep uf)

