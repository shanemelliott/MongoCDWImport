const config = require('./config');
const sql = require('mssql');

module.exports = {
  connect: ()=>{
    return new sql.connect(config)
  },
  query: (query,connection) => {
    return sql.connect(config).then((pool) => {
      return pool.request().query(query);
    });
  },
  url:  '' //MongoDB URL
  };
