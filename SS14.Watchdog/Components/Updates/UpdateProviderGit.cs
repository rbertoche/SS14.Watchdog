using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Mono.Unix;
using SS14.Watchdog.Components.ServerManagement;
using SS14.Watchdog.Configuration.Updates;

namespace SS14.Watchdog.Components.Updates
{
    public class UpdateProviderGit : UpdateProvider
    {
        private readonly ServerInstance _serverInstance;
        private readonly string _baseUrl;
        private readonly string _mediaUrl;
        private readonly string _branch;
        private readonly bool _hybridACZ;
        private readonly ILogger<UpdateProviderGit> _logger;
        private readonly string _repoPath;
        private readonly string _mediaPath;
        private readonly string _mediaGitPath;
        private readonly IConfiguration _configuration;
        private bool _newPackaging;

        public UpdateProviderGit(ServerInstance serverInstanceInstance, UpdateProviderGitConfiguration configuration, ILogger<UpdateProviderGit> logger, IConfiguration config)
        {
            _serverInstance = serverInstanceInstance;
            _baseUrl = configuration.BaseUrl;
            _mediaUrl = configuration.MediaUrl;
            _branch = configuration.Branch;
            _hybridACZ = configuration.HybridACZ;
            _logger = logger;
            _repoPath = Path.Combine(_serverInstance.InstanceDir, "source");
            _mediaPath = Path.Combine(_serverInstance.InstanceDir, "source", "Resources", "Media");
            _mediaGitPath = Path.Combine(_serverInstance.InstanceDir, "Media.git");
            _configuration = config;
        }

        private async Task<int> CommandHelper(string cd, string command, string[] args, CancellationToken cancel = default)
        {
            var si = new ProcessStartInfo {
                FileName = command, CreateNoWindow = true, UseShellExecute = true,
                WorkingDirectory = cd
            };
            // MSDN lied to me! https://docs.microsoft.com/en-us/dotnet/api/system.diagnostics.processstartinfo.argumentlist?view=net-6.0
            foreach (var s in args)
                si.ArgumentList.Add(s);
            var proc = new Process
            {
                StartInfo = si
            };
            proc.Start();
            await proc.WaitForExitAsync(cancel);
            if (cancel.IsCancellationRequested)
                return 127;
            return proc.ExitCode;
        }

        private async Task CommandHelperChecked(string reason, string cd, string command, string[] args, CancellationToken cancel = default)
        {
            int exitCode;
            try
            {
                exitCode = await CommandHelper(cd, command, args, cancel);
            }
            catch (Exception ex)
            {
                throw new Exception(reason, ex);
            }
            if (exitCode != 0)
            {
                throw new Exception(reason);
            }
        }

        private async Task<string> CommandHelperCheckedStdout(string reason, string cd, string command, string[] args)
        {
            try
            {
                var si = new ProcessStartInfo {
                    FileName = command, CreateNoWindow = true,
                    WorkingDirectory = cd,
                    RedirectStandardOutput = true
                };
                foreach (var s in args)
                    si.ArgumentList.Add(s);
                var proc = new Process
                {
                    StartInfo = si
                };
                proc.Start();
                var text = proc.StandardOutput.ReadToEnd();
                await proc.WaitForExitAsync();
                if (proc.ExitCode != 0)
                {
                    throw new Exception($"Exit code: {proc.ExitCode}");
                }
                return text;
            }
            catch (Exception ex)
            {
                throw new Exception(reason, ex);
            }
        }

        // Git actions

        private async Task<bool> GitCheckRepositoryValid()
        {
            try
            {
                return await CommandHelper(_repoPath, "git", new[] {"status"}) == 0;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private async Task<bool> GitFetchOrigin(CancellationToken cancel = default)
        {
            return await CommandHelper(_repoPath, "git", new[] {"fetch", _baseUrl, _branch}, cancel) == 0;
        }

        private async Task GitSwitchBranch(CancellationToken cancel = default)
        {
            await CommandHelperChecked($"Failed to switch to branch {_branch}!", _repoPath, "git",
                new[]{"switch", _branch}, cancel);
        }

        private async Task GitCheckedSubmoduleUpdate(CancellationToken cancel = default)
        {
            await CommandHelperChecked("Failed submodule update!", _repoPath, "git", new[] {"submodule", "update", "--init", "--depth=1", "--recursive", "--force"}, cancel);
        }

        private async Task<bool> GitFetchMedia(CancellationToken cancel = default)
        {
            return await CommandHelper(_mediaPath, "git", new[] {"--git-dir", _mediaGitPath, "fetch", _mediaUrl}, cancel) == 0;
        }

        private async Task GitCloneMedia(CancellationToken cancel = default)
	{
                if(!Directory.Exists(_mediaGitPath) && _mediaUrl != null) {
                    _logger.LogDebug($"Clone {_mediaPath} from {_mediaUrl}");
                    await CommandHelperChecked("Failed initial clone for Media!", "", "git", new[] {"--bare", "clone", "--depth=1", _mediaUrl, _mediaGitPath }, cancel);
                }
                if(!Directory.Exists(_mediaPath)) {
                    Directory.CreateDirectory(_mediaPath);
                    await GitResetMedia(cancel);
                }

        }

        private async Task GitResetMedia(CancellationToken cancel = default)
        {
            await CommandHelperChecked("Failed reset media", _mediaPath, "git", new[] {"--work-tree=.", "--git-dir", _mediaGitPath, "reset", "--hard"}, cancel);
        }

        private async Task GitResetToFetchHead(CancellationToken cancel = default)
        {
            await CommandHelperChecked("Failed reset to fetch-head", _repoPath, "git", new[] {"reset", "--hard", "FETCH_HEAD"}, cancel);
        }

        private async Task TryClone(CancellationToken cancel = default)
        {
            _logger.LogTrace("Cloning git repository...");

            if(Directory.Exists(_repoPath))
                Directory.Delete(_repoPath, true);

            try
            {
                // NOTE: These are expected to prepare everything including submodules,
                // because this is used for orbital nuking in the event of an update issue.
                // The --depth=1 is a performance cheat. Works though.
                await CommandHelperChecked("Failed initial clone!", "", "git", new[] {"clone", "--depth=1", _baseUrl, _repoPath}, cancel);
                await GitFetchOrigin(cancel);
                await GitResetToFetchHead(cancel);
                await GitCloneMedia(cancel);
                await GitCheckedSubmoduleUpdate(cancel);
            }
            catch (Exception)
            {
                if(Directory.Exists(_repoPath))
                    Directory.Delete(_repoPath, true);
                throw;
            }
        }

        private async Task<string?> GitHead(string head)
        {
            try
            {
                return (await CommandHelperCheckedStdout("", _repoPath, "git", new[] {"rev-parse", head})).Trim();
            }
            catch (Exception)
            {
                return null;
            }
        }

        private async Task<string?> MediaHead(string head)
        {
            try
            {
                return (await CommandHelperCheckedStdout("", _mediaPath, "git", new[] {"--git-dir", _mediaGitPath, "rev-parse", head})).Trim();
            }
            catch (Exception)
            {
                return null;
            }
        }


        // Updater and checker

        public override async Task<bool> CheckForUpdateAsync(string? currentVersion, CancellationToken cancel = default)
        {
            if ((!(await GitCheckRepositoryValid())) || currentVersion == null)
                return true;

            var update = false;

            if (!await GitFetchOrigin(cancel))
            {
                // Maybe the server's not up right now.
                return false;
            }
            await GitCloneMedia(cancel);

            if (!await GitFetchMedia(cancel))
            {
                // Maybe the server's not up right now.
                return false;
            }
            string mediaCurrentVersion = null;
            if (_mediaUrl != null){
                mediaCurrentVersion = await MediaHead("HEAD");
            }

            var head = await GitHead("HEAD");
            var fetchHead = await GitHead("FETCH_HEAD");
            if (head != fetchHead || currentVersion != fetchHead)
            {
                update = true;
            }

            if (_mediaUrl != null){
                var mediaHead = await MediaHead("HEAD");
                var mediaFetchHead = await MediaHead("FETCH_HEAD");
                if (mediaHead != mediaFetchHead || mediaCurrentVersion != mediaFetchHead)
                {
                    update = true;
                }

                _logger.LogInformation($"Update check: {head ?? "No head"}, {fetchHead ?? "No fetch-head"} " +
                                        $"{mediaHead ?? "No media head"}, {mediaFetchHead ?? "No media fetch-head"} - updating: {update}");
            } else {
                _logger.LogInformation($"Update check: {head ?? "No head"}, {fetchHead ?? "No fetch-head"} - updating: {update}");
            }
            return update;
        }

        public override async Task<string?> RunUpdateAsync(string? currentVersion, string binPath, CancellationToken cancel = default)
        {
            try
            {
                var isFresh = false;
                if (!await GitCheckRepositoryValid() || currentVersion == null)
                {
                    await TryClone(cancel);
                    isFresh = true;
                }

                _logger.LogTrace("Updating...");

                // NOTE: A race condition could happen here if an update check is performed while we're running an update.
                // The solution is that the update check solely occurs on FETCH_HEAD.
                // Therefore, the `git reset --hard FETCH_HEAD` is assumed to either provide one consistent HEAD or error.

                if (!isFresh)
                {
                    try
                    {
                        // Don't allow these to be cancelled as they could probably corrupt the repository.
                        if (!(await GitFetchOrigin(cancel)))
                            throw new Exception("Could not fetch origin");
                        await GitResetToFetchHead(cancel);
                        if (!(await GitFetchMedia(cancel)))
                            throw new Exception("Could not fetch media origin");
                        await GitResetMedia(cancel);
                        await GitCheckedSubmoduleUpdate(cancel);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning($"Failed git submodule update: {ex}");
                        _logger.LogWarning($"Nuking the repository from orbit to recover!");
                        await TryClone(cancel);
                    }
                }

                var actualConfirmedHead = await GitHead("HEAD");
                if (actualConfirmedHead == null)
                    throw new Exception("Head disappeared!");

                _logger.LogDebug($"Went from {currentVersion} to {actualConfirmedHead}");

                // Now we build and package it.

                // Platform to build the server for.

                // Where the server zip will be created by the server build script.
                var serverPackage = Path.Combine(_repoPath, "release", ServerZipName);
                var serverPlatform = GetHostSS14RID();

                // check for the new packaging system, else it will fallback to the old python one
                if (Directory.Exists(Path.Combine(_repoPath, "Content.Packaging")))
                {
                    _newPackaging = true;

                    await CommandHelperChecked("Failed to dotnet restore", _repoPath, "dotnet", new[] { "restore" }, cancel);

                    await CommandHelperChecked("Failed to build Content Packaging",
                        _repoPath, "dotnet", new[] { "build", "Content.Packaging","--configuration", "Release", "--no-restore", "/m" }, cancel);
                }
                else
                    _newPackaging = false;

                if (_hybridACZ)
                {
                    if (_newPackaging)
                    {
                        await CommandHelperChecked("Failed to build Hybrid ACZ package with Content Packaging",
                            _repoPath, "dotnet", new[] { "run", "--project", "Content.Packaging", "server", "--platform", serverPlatform, "--hybrid-acz" }, cancel);
                    }
                    else
                        await CommandHelperChecked("Failed to build Hybrid ACZ package with Python", _repoPath, "python", new[] {"Tools/package_server_build.py", "--hybrid-acz", "-p", serverPlatform}, cancel);
                }
                else
                {
                    var binariesPath = Path.Combine(_serverInstance.InstanceDir, "binaries");

                    // If you get an error here: You need a BaseUrl in the root of appsettings.yml that represents the public URL of the watchdog server.
                    var binariesRoot = new Uri(new Uri(_configuration["BaseUrl"]!),
                        $"instances/{_serverInstance.Key}/binaries/");

                    _logger.LogTrace("Building server packages...");

                    if (_newPackaging)
                    {
                        await CommandHelperChecked("Failed to build server packages with Content Packaging",
                            _repoPath, "dotnet", new[] { "run", "--project", "Content.Packaging", "server", "--platform", serverPlatform}, cancel);
                    }
                    else
                        await CommandHelperChecked("Failed to build server packages with Python", _repoPath, "python", new[] {"Tools/package_server_build.py", "-p", serverPlatform}, cancel);


                    _logger.LogTrace("Building client packages...");

                    if (_newPackaging)
                    {
                        await CommandHelperChecked("Failed to build client packages with Content Packaging",
                            _repoPath, "dotnet", new[] { "run", "--project", "Content.Packaging", "client", "--no-wipe-release"}, cancel);
                    }
                    else
                        await CommandHelperChecked("Failed to build client packages", _repoPath, "python", new[] {"Tools/package_client_build.py"}, cancel);

                    File.Move(Path.Combine(_repoPath, "release", ClientZipName), Path.Combine(binariesPath, ClientZipName), true);
                    // Unless using Hybrid ACZ, a build.json file must be written.
                    await using (var stream = File.Open(serverPackage, FileMode.Open))
                    {
                        using (var archive = new ZipArchive(stream, ZipArchiveMode.Update))
                        {
                            var build = new Build()
                            {
                                Download = new Uri(binariesRoot, ClientZipName).ToString(),
                                Hash = GetFileHash(Path.Combine(binariesPath, ClientZipName)),
                                Version = actualConfirmedHead,
                                // Use ACZ version auto-detection.
                                EngineVersion = "",
                                ForkId = _baseUrl,
                            };
                            ZipArchiveEntry readmeEntry = archive.CreateEntry("build.json");
                            await using (StreamWriter writer = new StreamWriter(readmeEntry.Open()))
                            {
                                await writer.WriteLineAsync(JsonSerializer.Serialize(build));
                            }
                        }
                    }
                }

                _logger.LogTrace("Applying server update.");

                if (Directory.Exists(binPath))
                {
                    Directory.Delete(binPath, true);
                }

                Directory.CreateDirectory(binPath);

                _logger.LogTrace("Extracting zip file");

                // Actually extract.
                await using (var stream = File.Open(serverPackage, FileMode.Open))
                {
                    using (var archive = new ZipArchive(stream, ZipArchiveMode.Read))
                    {
                        archive.ExtractToDirectory(binPath);
                    }
                }

                // Remove the package now that it's extracted.
                File.Delete(serverPackage);

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                    RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                {
                    // chmod +x Robust.Server

                    var rsPath = Path.Combine(binPath, "Robust.Server");
                    if (File.Exists(rsPath))
                    {
                        var f = new UnixFileInfo(rsPath);
                        f.FileAccessPermissions |=
                            FileAccessPermissions.UserExecute | FileAccessPermissions.GroupExecute |
                            FileAccessPermissions.OtherExecute;
                    }
                }

                // ReSharper disable once RedundantTypeArgumentsOfMethod
                return actualConfirmedHead;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Failed to run update!");

                return null;
            }
        }

        private class Build
        {
            [JsonPropertyName("download")]
            public string Download { get; set; } = default!;

            [JsonPropertyName("hash")]
            public string Hash { get; set; } = default!;

            [JsonPropertyName("version")]
            public string Version { get; set; } = default!;

            [JsonPropertyName("engine_version")]
            public string EngineVersion { get; set; } = default!;

            [JsonPropertyName("fork_id")]
            public string ForkId { get; set; } = default!;
        }
    }
}
