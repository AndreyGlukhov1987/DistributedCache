using CacheServer.Common;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CacheServerConsole
{
    public class Cache
    {
        public class MinMax
        {
            public int Min { get; set; }
            public int Max { get; set; }
        }

        private const int MAX_NUM_OF_KEYS = 100;
        private const string LOG_FILE = "Messages.log";
        private const string MIRROR_LOG_FILE = "MirrorMessages.log";
        private const string LOG_FORMAT = "{0} {1} {2}";
        private const string ADD_OPERATION = "ADD";
        private const string DELETE_OPERATION = "DELETE";
        private const string EDIT_OPERATION = "EDIT";

        private Dictionary<int, string> _cache = null;
        private Dictionary<int, string> _mirrorCache = null;
        private Dictionary<string, HubConnection> _serverHubs = null;
        private Dictionary<MinMax, string> _keyRangeServer;
        private HubConnection _masterConnection = null;

        private static string _masterUrl = null;
        private static IConfigurationRoot _config = null;

        private int _minKey = 0;
        private int _maxKey = MAX_NUM_OF_KEYS;

        public Cache()
        {
            _cache = new Dictionary<int, string>();
            _mirrorCache = new Dictionary<int, string>();
            _serverHubs = new Dictionary<string, HubConnection>();
            _keyRangeServer = new Dictionary<MinMax, string>();

            _config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("hosting.json", optional: true)
                .Build();

            RestoreCache(LOG_FILE);
            RestoreCache(MIRROR_LOG_FILE);

            ConnectToMaster();
        }

        private async void ConnectToMaster()
        {
            //if has master section then connect
            _masterUrl = _config.GetSection("Master").Value;
            if (_masterUrl != null)
            {
                _masterConnection = new HubConnectionBuilder().WithUrl(_config["Master"] + "/cacheHub").Build();
                _masterConnection.ServerTimeout = new System.TimeSpan(1, 0, 0);
                _masterConnection.Closed += async (error) =>
                {
                    await Task.Delay(5000);
                    await _masterConnection.StartAsync();
                };

                await _masterConnection.StartAsync();
                if (_masterConnection.State == HubConnectionState.Connected)
                {
                    await _masterConnection.InvokeAsync("AddServer", _config["hostUrl"]);
                }
            }
        }

        private void RestoreCache(string path)
        {
            if (System.IO.File.Exists(path))
            {
                using (var fileStream = System.IO.File.OpenText(path))
                {
                    string line = "";
                    while ((line = fileStream.ReadLine()) != null)
                    {
                        var vals = line.Split(" ");
                        int key = int.Parse(vals[1]);
                        if (vals[0] == ADD_OPERATION)
                            _cache.Add(key, vals[2]);
                        else if (vals[0] == DELETE_OPERATION)
                        {
                            if (_cache.ContainsKey(key))
                                _cache.Remove(key);
                        }
                        else if (vals[0] == EDIT_OPERATION)
                        {
                            if (_cache.ContainsKey(key))
                                _cache[key] = vals[2];
                        }
                    }
                }
            }
            else
            {
                if (!System.IO.File.Exists(path))
                {
                    System.IO.File.Create(path).Close();
                }
            }
        }

        private void WriteLog(string operation, DataObject obj, bool mirror)
        {
            using (StreamWriter sw = System.IO.File.AppendText(mirror ? MIRROR_LOG_FILE : LOG_FILE))
            {
                sw.WriteLine(string.Format(LOG_FORMAT, operation, obj.Key, obj.Data));
            }
        }
        //add new server to pool
        //connect and send information for back connection
        public async void AddServer(string url)
        {
            if (!_serverHubs.ContainsKey(url))
            {
                var connection = new HubConnectionBuilder().WithUrl(url + "/cacheHub").Build();
                connection.ServerTimeout = new System.TimeSpan(1, 0, 0);
                connection.Closed += async (error) =>
                {
                    //if connection lost, then redistibute cache between working servers
                    redistributeCache();
                };

                await connection.StartAsync();

                foreach (var server in _serverHubs)
                {
                    await server.Value.InvokeAsync("AddServer", url);
                }

                _serverHubs.Add(url, connection);

                redistributeCache();
            }
        }

        private async void redistributeCache()
        {
            //it does only master
            if (_masterUrl == null)
            {
                int keysPerServer = MAX_NUM_OF_KEYS / (_serverHubs.Count + 1);
                _maxKey = keysPerServer;
                int nextMin = _maxKey + 1;
                int nextMax = _maxKey + keysPerServer;

                _keyRangeServer.Add(new MinMax { Min = _minKey, Max = _maxKey }, _config["hostUrl"]);

                foreach (var server in _serverHubs)
                {
                    await server.Value.InvokeAsync("SetMinMaxKeys", new MinMax() { Min = nextMin, Max = nextMax });
                    //send map to each server
                    foreach (var keyServer in _keyRangeServer)
                    {
                        await server.Value.InvokeAsync("SetMinMaxKeysMap", keyServer);
                    }
                    nextMin = nextMax + 1;
                    nextMax = nextMax + keysPerServer;
                }
            }
        }

        public void SetMinMaxKeys(MinMax minMax)
        {
            _minKey = minMax.Min;
            _maxKey = minMax.Max;
        }

        public void SetMinMaxKeysMap(KeyValuePair<MinMax, string> pair)
        {
            _keyRangeServer.Add(pair.Key, pair.Value);
        }

        public void AddMirrorData(DataObject data)
        {
            if (!_mirrorCache.ContainsKey(data.Key))
            {
                _mirrorCache.Add(data.Key, data.Data);
                WriteLog(ADD_OPERATION, data, true);
            }
        }

        public async void AddData(DataObject data)
        {
            if (!_cache.ContainsKey(data.Key))
            {
                _cache.Add(data.Key, data.Data);
                WriteLog(ADD_OPERATION, data, false);
                //if last server then mirror to first
                var mirrorConnection = GetMirrorConnection();
                if(mirrorConnection != null)
                    mirrorConnection.InvokeAsync("AddMirrorData", data);
            }
        }

        private HubConnection GetMirrorConnection()
        {
            if (_maxKey == MAX_NUM_OF_KEYS)
            {
                return _masterConnection;
            }
            else
            {
                var keyServer = _keyRangeServer.Keys.Where(key => key.Min == (_maxKey + 1));
                if (keyServer.Any())
                {
                    return _serverHubs[_keyRangeServer[keyServer.First()]];
                }
            }

            return null;
        }

        public void DeleteData(DataObject data)
        {
            //delete from main cache
            if (_cache.ContainsKey(data.Key))
            {
                _cache.Remove(data.Key);
                WriteLog(DELETE_OPERATION, data, false);
            }

            //if mirror has key by some reason remove it
            if(_mirrorCache.ContainsKey(data.Key))
            {
                _mirrorCache.Remove(data.Key);
                WriteLog(DELETE_OPERATION, data, true);
            }
            //remove from mirror
            var mirrorConnection = GetMirrorConnection();
            if(mirrorConnection != null)
                mirrorConnection.InvokeAsync("DeleteData", data);
        }

        public async Task<DataObject> GetData(int key)
        {
            //if found in cache
            if (_cache.ContainsKey(key))
                return new DataObject() { Key = key, Data = _cache[key] };
            else if(_mirrorCache.ContainsKey(key))
                return new DataObject() { Key = key, Data = _mirrorCache[key] };
            //if key from my range but doesn't exist in cache
            else if(_minKey <= key && key < _maxKey)
            {
                //try to find on other servers
                foreach(var server in _serverHubs)
                {
                    if(server.Value.InvokeAsync<bool>("HasValue", key).Result)
                    {
                        var res = server.Value.InvokeAsync<DataObject>("GetData", key).Result;
                        //save result in main cache
                        AddData(res);
                        //and return
                        return res;
                    }
                }
            }
            else
            {
                //redirect to correct server
                foreach(var server in _keyRangeServer)
                {
                    if(server.Key.Min <= key && key < server.Key.Max)
                    {
                        return _serverHubs[server.Value].InvokeAsync<DataObject>("GetData", key).Result;
                    }
                }
            }

            return null;
        }

        public bool HasData(int key)
        {
            return _cache.ContainsKey(key) || _mirrorCache.ContainsKey(key);
        }
    }
}
