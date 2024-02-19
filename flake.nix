{
  description =
    "Garage, an S3-compatible distributed object store for self-hosted deployments";

  # Nixpkgs 23.11 as of 2024-02-07, has rustc v1.73
  inputs.nixpkgs.url =
    "github:NixOS/nixpkgs/nixos-unstable";

  inputs.flake-compat.url = "github:nix-community/flake-compat";

  inputs.cargo2nix = {
    # As of 2022-10-18: two small patches over unstable branch, one for clippy and one to fix feature detection
    url = "github:Alexis211/cargo2nix/a7a61179b66054904ef6a195d8da736eaaa06c36";

    # As of 2023-04-25:
    # - my two patches were merged into unstable (one for clippy and one to "fix" feature detection)
    # - rustc v1.66
    # url = "github:cargo2nix/cargo2nix/8fb57a670f7993bfc24099c33eb9c5abb51f29a2";

    # Rust overlay as of 2024-02-07
    inputs.rust-overlay.url =
      "github:oxalica/rust-overlay/7a94fe7690d2bdfe1aab475382a505e14dc114a6";

    inputs.nixpkgs.follows = "nixpkgs";
    inputs.flake-compat.follows = "flake-compat";
  };

  inputs.flake-utils.follows = "cargo2nix/flake-utils";

  outputs = { self, nixpkgs, cargo2nix, flake-utils, ... }:
    let
      git_version = self.lastModifiedDate;
      compile = import ./nix/compile.nix;
    in
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages =
          let
            packageFor = target: (compile {
              inherit system git_version target;
              pkgsSrc = nixpkgs;
              cargo2nixOverlay = cargo2nix.overlays.default;
              release = true;
            }).workspace.garage { compileMode = "build"; };
          in
          rec {
            # default = native release build
            default = packageFor null;
            # other = cross-compiled, statically-linked builds
            amd64 = packageFor "x86_64-unknown-linux-musl";
            i386 = packageFor "i686-unknown-linux-musl";
            arm64 = packageFor "aarch64-unknown-linux-musl";
            arm = packageFor "armv6l-unknown-linux-musl";

            jepsen-garage = pkgs.callPackage (
              { runCommand, makeWrapper, fetchMavenArtifact, fetchgit, lib, clojure }:

              let cljsdeps = import ./script/jepsen.garage/deps.nix { inherit (pkgs) fetchMavenArtifact fetchgit lib; };
                  classp  = cljsdeps.makeClasspaths {};

              in runCommand "jepsen.garage" {
                nativeBuildInputs = [ makeWrapper ];
              } ''
                mkdir -p $out/share
                cp -r ${./script/jepsen.garage/src} $out/share/src
                substituteInPlace $out/share/src/jepsen/garage/daemon.clj --replace "https://garagehq.deuxfleurs.fr/_releases/" "http://runner/"
                makeWrapper ${clojure}/bin/clojure $out/bin/jepsen.garage --add-flags "-Scp $out/share/src:${classp} -m jepsen.garage"
              ''
              ) {};

            nixosTest = pkgs.nixosTest ({lib, pkgs, ... }: let

              snakeOilPrivateKey = pkgs.writeText "privkey.snakeoil" ''
                -----BEGIN EC PRIVATE KEY-----
                MHcCAQEEIHQf/khLvYrQ8IOika5yqtWvI0oquHlpRLTZiJy5dRJmoAoGCCqGSM49
                AwEHoUQDQgAEKF0DYGbBwbj06tA3fd/+yP44cvmwmHBWXZCKbS+RQlAKvLXMWkpN
                r1lwMyJZoSGgBHoUahoYjTh9/sJL7XLJtA==
                -----END EC PRIVATE KEY-----
              '';
              snakeOilPublicKey = pkgs.lib.concatStrings [
                "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHA"
                "yNTYAAABBBChdA2BmwcG49OrQN33f/sj+OHL5sJhwVl2Qim0vkUJQCry1zFpKTa"
                "9ZcDMiWaEhoAR6FGoaGI04ff7CS+1yybQ= snakeoil"
              ];

              nodeFn = n: lib.nameValuePair "n${toString (1+n)}" {
                services.openssh.enable = true;
                users.users.root.openssh.authorizedKeys.keys = [snakeOilPublicKey];
                environment.systemPackages = [
                  pkgs.wget
                  (pkgs.writeShellScriptBin "apt-get" ''
                    exit 0
                  '')
                  (lib.hiPrio (pkgs.writeShellScriptBin "dpkg" ''
                    exit 0
                  ''))
                  (lib.hiPrio (pkgs.writeShellScriptBin "ntpdate" ''
                    hwclock -s
                  ''))

                  (pkgs.dpkg.overrideAttrs (old: {
                    configureFlags = lib.remove "--disable-start-stop-daemon" old.configureFlags;
                  }))

                  pkgs.gcc
                ];
              };
              nodeCount = 7;

            in {
              name = "garage-jepsen";

              nodes = {
                runner = { nodes, ... }: {
                  environment.systemPackages = [
                    jepsen-garage

                    (pkgs.writeShellScriptBin "git" ''
                      echo 0000000000000000000000000000000000000000
                    '')

                    pkgs.gnuplot
                  ];
                  networking.firewall.allowedTCPPorts = [ 80 ];
                  environment.etc.jepsen-nodes.text = lib.concatStringsSep "\n" (lib.genList (n: nodes."n${toString (n+1)}".networking.primaryIPAddress) nodeCount);

                  services.nginx = {
                    enable = true;
                    virtualHosts.default = {
                      locations."/v0.9.0/x86_64-unknown-linux-musl/".alias = "${packageFor pkgs.pkgsStatic.stdenv.hostPlatform.config}/bin/";
                    };
                  };
                };
              } // lib.listToAttrs (lib.genList nodeFn nodeCount);

              testScript = ''
                start_all()

                n1.wait_for_unit("sshd.service")

                runner.succeed("mkdir ~/.ssh")
                runner.succeed(
                    "cat ${snakeOilPrivateKey} > ~/.ssh/id_ecdsa"
                )
                runner.succeed("chmod 600 ~/.ssh/id_ecdsa")

                runner.succeed("source <(ssh-agent) && ssh-add && jepsen.garage test --nodes-file /etc/jepsen-nodes --time-limit 60 --rate 100  --concurrency 20 --workload reg1 --ops-per-key 100")
              '';
            });
          };

        # ---- development shell, for making native builds only ----
        devShells =
          let
            shellWithPackages = (packages: (compile {
              inherit system git_version;
              pkgsSrc = nixpkgs;
              cargo2nixOverlay = cargo2nix.overlays.default;
            }).workspaceShell { inherit packages; });
          in
          {
            default = shellWithPackages
              (with pkgs; [
                rustfmt
                clang
                mold
              ]);

            # import the full shell using `nix develop .#full`
            full = shellWithPackages (with pkgs; [
              rustfmt
              clang
              mold
              # ---- extra packages for dev tasks ----
              cargo-audit
              cargo-outdated
              cargo-machete
              nixpkgs-fmt
            ]);
          };
      });
}
