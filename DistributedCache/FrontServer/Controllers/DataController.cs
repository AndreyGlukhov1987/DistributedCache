using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CacheServer.Common;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR.Client;

namespace FrontServer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DataController : ControllerBase
    {
        List<HubConnection> _hubConnections;
        public DataController()
        {
            _hubConnections = new List<HubConnection>();
            var connection = new HubConnectionBuilder().WithUrl("http://localhost:5002/cacheHub").Build();
            connection.StartAsync();
            _hubConnections.Add(connection);
        }

        private HubConnection GetServerForKey()
        {
            return _hubConnections.First();
        }
        // GET: api/Data/5
        [HttpGet("{key}")]
        public Task<DataObject> Get(int key)
        {
            var backServer = GetServerForKey();
            if (backServer == null)
                return null;

            return backServer.InvokeAsync<DataObject>("GetData", key);
        }

        // POST api/data?Key=1&Value=2
        [HttpPost]
        public void Post(int key, string value)
        {
            var backServer = GetServerForKey();
            if (backServer == null)
                return;

            backServer.InvokeAsync("AddData", new DataObject() { Key = key, Data = value });
        }

        // PUT: api/Data/5
        [HttpPut("{key}")]
        public void Put(int id, string value)
        {
            var backServer = GetServerForKey();
            if (backServer == null)
                return;

            //backServer.InvokeAsync("EditData", new DataObject() { Key = key, Data = value });
        }

        // DELETE: api/ApiWithActions/5
        [HttpDelete("{key}")]
        public void Delete(int key)
        {
            var backServer = GetServerForKey();
            if (backServer == null)
                return;

            backServer.InvokeAsync("DeleteData", key);
        }
    }
}
