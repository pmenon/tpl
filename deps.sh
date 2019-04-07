#!/bin/bash

## =================================================================
## TPL PACKAGE INSTALLATION
##
## This script will install all packages needed to run TPL.
##
## Supported environments:
##  * Ubuntu 18.04
##  * MacOS
## =================================================================

main() {
  set -o errexit

  echo "PACKAGES WILL BE INSTALLED. THIS MAY BREAK YOUR EXISTING TOOLCHAIN."
  echo "YOU ACCEPT ALL RESPONSIBILITY BY PROCEEDING."

  read -p "Proceed? [Y/n] : " yn
  case $yn in
    Y|y) install ;;
    *) ;;
  esac

  echo "Script complete."
}

install() {
  set -x
  UNAME=$(uname | tr "[:lower:]" "[:upper:]" )

  case $UNAME in
    DARWIN) install_mac ;;

    LINUX)
      version=$(cat /etc/os-release | grep VERSION_ID | cut -d '"' -f 2)
      case $version in
        18.10) install_linux ;;
        18.04) install_linux ;;
        *) give_up ;;
      esac
      ;;

    *) give_up ;;
  esac
}

give_up() {
  set +x
  echo "Unsupported distribution '$UNAME'"
  echo "Please contact our support team for additional help."
  echo "Be sure to include the contents of this message."
  echo "Platform: $(uname -a)"
  echo
  echo "https://github.com/pmenon/tpl/issues"
  echo
  exit 1
}

install_mac() {
  # Install Homebrew.
  if test ! $(which brew); then
    echo "Installing Homebrew (https://brew.sh/)"
    ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
  fi
  # Update Homebrew.
  brew update
  # Install packages.
  brew ls --versions cmake || brew install cmake
  brew ls --versions git || brew install git
  (brew ls --versions llvm | grep 7) || brew install llvm@7
}

install_linux() {
  # Update apt-get.
  apt-get -y update
  # Install packages.
  apt-get -y install \
      build-essential \
      clang-format-7 \
      cmake \
      git \
      g++-7 \
      clang-7 \
      llvm-7
}

main "$@"
