using CacheServer.Common;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Generic;
using System.Threading.Tasks;
using static CacheServerConsole.Cache;

namespace CacheServerConsole.Hubs
{
    public class CacheHub : Hub
    {
        Cache _cache;
        public CacheHub(Cache cache)
        {
            _cache = cache;
        }

        //add new server to pool
        //connect and send information for back connection
        public void AddServer(string url)
        {
            _cache.AddServer(url);
        }

        public void SetMinMaxKeys(MinMax minMax)
        {
            _cache.SetMinMaxKeys(minMax);
        }

        public void SetMinMaxKeysMap(KeyValuePair<MinMax, string> pair)
        {
            _cache.SetMinMaxKeysMap(pair);
        }

        public void AddData(DataObject data)
        {
            _cache.AddData(data);
        }

        public void DeleteData(DataObject data)
        {
            _cache.DeleteData(data);
        }

        public async Task<DataObject> GetData(int key)
        {
            return _cache.GetData(key).Result;
        }

        public bool HasData(int key)
        {
            return _cache.HasData(key);
        }
    }
}
