let
  pkgs = import (builtins.fetchGit {
    name = "nixpkgs-unstable";
    url = "https://github.com/nixos/nixpkgs/";
    # `git ls-remote https://github.com/nixos/nixpkgs nixos-unstable`
    ref = "refs/heads/nixpkgs-unstable";
    rev = "bd4dffcdb7c577d74745bd1eff6230172bd176d5";
  }) {};

  mdox = pkgs.buildGoModule {
    name = "mdox";
    src = pkgs.fetchFromGitHub {
      owner = "bwplotka";
      repo = "mdox";
      rev = "v0.9.0";
      sha256 = "sha256-vY8klEOCpfvTEkAPziMdFPLaDVpI3Wvhu6unUqGurOw=";
    };
    vendorSha256 = "sha256-skk+jt7ylCmHss0VyhJqsFvXleU+34fK9AepR9u2yTE=";
    doCheck = false;
  };

in

pkgs.mkShell {
  buildInputs = [
    mdox
    pkgs.golangci-lint
  ];
}
