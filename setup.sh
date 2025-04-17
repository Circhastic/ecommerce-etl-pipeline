#!/bin/bash

set -e

echo "Starting setup for Docker, Terraform, and Astro CLI..."

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

echo "Updating package list..."
sudo apt-get update -y

echo "Installing prerequisites (curl, gnupg, apt-transport-https, ca-certificates, software-properties-common, lsb-release)..."
sudo apt-get install -y curl gnupg apt-transport-https ca-certificates software-properties-common lsb-release

if command_exists docker; then
  echo "Docker is already installed."
else
  echo "Installing Docker..."
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  sudo chmod a+r /etc/apt/keyrings/docker.gpg

  # Add the Docker repository to Apt sources
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt-get update -y

  # Install Docker packages
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

  echo "Adding current user ($USER) to the docker group..."
  sudo usermod -aG docker $USER

  echo "Starting and enabling Docker service..."
  sudo systemctl start docker
  sudo systemctl enable docker

  echo "Docker installation complete."
  echo "IMPORTANT: You need to log out and log back in (or run 'newgrp docker') for docker group changes to take effect!"
fi

# --- Install Terraform ---
if command_exists terraform; then
    echo "Terraform is already installed."
else
    echo "Installing Terraform..."
    wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg

    gpg --no-default-keyring --keyring /usr/share/keyrings/hashicorp-archive-keyring.gpg --fingerprint

    echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list

    sudo apt-get update -y
    sudo apt-get install -y terraform

    echo "Terraform installation complete."
fi

# --- Install Astro CLI ---
if command_exists astro; then
    echo "Astro CLI is already installed."
else
    echo "Installing Astro CLI..."
    curl -sSL install.astronomer.io | sudo bash -s
    echo "Astro CLI installation complete."
fi

echo "-------------------------------------------"
echo "Setup script finished!"
echo "Installed versions:"
docker --version || echo "Docker not found or not working yet."
terraform --version || echo "Terraform not found."
astro version || echo "Astro CLI not found."
echo "-------------------------------------------"
echo "IMPORTANT REMINDER:"
echo "If Docker was installed, please LOG OUT and LOG BACK IN"
echo "or run 'newgrp docker' in your current shell"
echo "before running Docker commands without sudo (needed for Astro CLI)."
echo "-------------------------------------------"