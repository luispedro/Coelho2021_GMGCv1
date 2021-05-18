with (import <nixpkgs> {});

stdenv.mkDerivation {

  name = "redundant100env";

  buildInputs = with haskell.packages.ghc802; [
    stack
    wget
    ghc
    m4
  ];
  propagatedBuildInputs = [
    zlib
    bzip2
    lzma
  ];

  STACK_IN_NIX_EXTRA_ARGS
      = " --extra-lib-dirs=${zlib.out}/lib"
      + " --extra-lib-dirs=${bzip2.out}/lib"
      + " --extra-lib-dirs=${lzma.out}/lib"
      + " --extra-include-dirs=${zlib.out}/include"
      + " --extra-include-dirs=${bzip2.out}/include"
      + " --extra-include-dirs=${lzma.out}/include"
  ;
  extraCmds = ''
    export LD_LIBRARY_PATH+=:${zlib.out}/lib
    export LD_LIBRARY_PATH+=:${bzip2.out}/lib
    export LD_LIBRARY_PATH+=:${lzma.out}/lib
  '';
  shellHook = ''
    export LD_LIBRARY_PATH+=:${zlib.out}/lib
    export LD_LIBRARY_PATH+=:${bzip2.out}/lib
    export LD_LIBRARY_PATH+=:${lzma.out}/lib

  '';
}
