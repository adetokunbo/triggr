{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  packages = [
    pkgs.basedpyright
    (pkgs.python313.withPackages (ps: with ps; [
      pytest
      pytest-asyncio
      pytest-cov
    ]))
  ];
}
