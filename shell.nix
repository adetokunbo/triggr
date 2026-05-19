{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  packages = [
    (pkgs.python313.withPackages (ps: with ps; [
      pytest
      pytest-asyncio
    ]))
  ];
}
