{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  packages = [
    pkgs.basedpyright
    pkgs.ruff
    pkgs.pre-commit
    (pkgs.python313.withPackages (ps: with ps; [
      pytest
      pytest-asyncio
      pytest-cov
    ]))
  ];
}
