module.exports = {
    server: "", 
    database: "",
    domain:'',
    user:'',
    password:'',
    options: {
      enableArithAbort: true,
      trustedConnection: true,
      trustServerCertificate: true
    },
    pool: {
      max: process.env.DB_MAX_POOL || 5000,
      min: 0,
      idleTimeoutMillis: 90000
    } 
};



  
