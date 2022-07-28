package commands

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/grafana/grafana/pkg/cmd/grafana-cli/models"
	"github.com/grafana/grafana/pkg/cmd/grafana-cli/services"
	"github.com/grafana/grafana/pkg/cmd/grafana-cli/utils"
	"github.com/grafana/grafana/pkg/plugins/repository"
	"github.com/grafana/grafana/pkg/plugins/repository/service"
	"github.com/grafana/grafana/pkg/plugins/storage"
)

func validateInput(c utils.CommandLine, pluginFolder string) error {
	arg := c.Args().First()
	if arg == "" {
		return errors.New("please specify plugin to install")
	}

	pluginsDir := c.PluginDirectory()
	if pluginsDir == "" {
		return errors.New("missing pluginsDir flag")
	}

	fileInfo, err := os.Stat(pluginsDir)
	if err != nil {
		if err = os.MkdirAll(pluginsDir, os.ModePerm); err != nil {
			return fmt.Errorf("pluginsDir (%s) is not a writable directory", pluginsDir)
		}
		return nil
	}

	if !fileInfo.IsDir() {
		return errors.New("path is not a directory")
	}

	return nil
}

func (cmd Command) installCommand(c utils.CommandLine) error {
	pluginFolder := c.PluginDirectory()
	if err := validateInput(c, pluginFolder); err != nil {
		return err
	}

	pluginID := c.Args().First()
	version := c.Args().Get(1)
	return InstallPlugin(context.Background(), pluginID, version, c)
}

// InstallPlugin downloads the plugin code as a zip file from the Grafana.com API
// and then extracts the zip into the plugin's directory.
func InstallPlugin(ctx context.Context, pluginID, version string, c utils.CommandLine) error {
	skipTLSVerify := c.Bool("insecure")
	repo := service.New(skipTLSVerify, c.PluginRepoURL(), services.Logger)

	compatOpts := repository.CompatabilityOpts{
		GrafanaVersion: services.GrafanaVersion,
		OS:             runtime.GOOS,
		Arch:           runtime.GOARCH,
	}

	pluginZipURL := c.PluginURL()
	var archive *repository.PluginArchive
	var err error
	if pluginZipURL != "" {
		archive, err = repo.GetPluginArchiveByURL(ctx, pluginZipURL, compatOpts)
		if err != nil {
			return err
		}
	} else {
		archive, err = repo.GetPluginArchive(ctx, pluginID, version, compatOpts)
		if err != nil {
			return err
		}
	}

	pluginFs := storage.NewFileSystem(services.Logger, c.PluginDirectory())
	extractedArchive, err := pluginFs.Add(ctx, pluginID, archive.File)
	if err != nil {
		return err
	}

	for _, dep := range extractedArchive.Dependencies {
		services.Logger.Info("Fetching %s dependency...", dep.ID)
		d, err := repo.GetPluginArchive(ctx, dep.ID, dep.Version, compatOpts)
		if err != nil {
			return fmt.Errorf("%v: %w", fmt.Sprintf("failed to download plugin %s from repository", dep.ID), err)
		}

		_, err = pluginFs.Add(ctx, dep.ID, d.File)
		if err != nil {
			return err
		}
	}
	return err
}

func osAndArchString() string {
	osString := strings.ToLower(runtime.GOOS)
	arch := runtime.GOARCH
	return osString + "-" + arch
}

func supportsCurrentArch(version *models.Version) bool {
	if version.Arch == nil {
		return true
	}
	for arch := range version.Arch {
		if arch == osAndArchString() || arch == "any" {
			return true
		}
	}
	return false
}

func latestSupportedVersion(plugin *models.Plugin) *models.Version {
	for _, v := range plugin.Versions {
		ver := v
		if supportsCurrentArch(&ver) {
			return &ver
		}
	}
	return nil
}
