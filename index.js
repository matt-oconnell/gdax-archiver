const express = require('express')
const axios = require('axios')
const mysql = require('mysql')
const creds = require('./creds.js')

const app = express()
const coins = ['btc', 'ltc', 'eth']

app.get('/', async (req, res) => {
  let msg = ''
  for (let coin of coins) {
    let { data } = await axios.get(`https://api.gdax.com/products/${coin.toUpperCase()}-USD/candles/1m`)
    msg = await store(data, coin)
  }
  res.send(msg)
})

app.listen(3000, async() => {
  console.log('Booted');
})

const getMaxTimestamp = (connection, coin) => new Promise((res, rej) => {
  const selector = 'MAX(timestamp)'
  connection.query(`SELECT ${selector} FROM ${coin}`, (e, queryResult) => {
    if (e) {
      rej(e)
    }
    res(queryResult[0][selector])
  })
})

const storeNewDataPoints = (connection, coin, newDataPoints) => new Promise((res, rej) => {
  const query = `INSERT INTO ${coin} (timestamp, lo, hi, open, close, volume) VALUES ?`
  connection.query(query, [newDataPoints], (e) => {
    if (e) {
      rej(e)
    }
    res()
  })
})

async function store(dataPoints, coin, cb) {
  const connection = mysql.createConnection(creds);
  connection.connect()

  const maxTimestamp = await getMaxTimestamp(connection, coin)  
  
  if (dataPoints[0][0] === maxTimestamp) {
    return 'No new data'
  }

  let capIndex = 0
  if (!maxTimestamp) {
    capIndex = dataPoints.length
  }
  else {
    while (dataPoints[capIndex][0] > maxTimestamp && capIndex < dataPoints.length) {
      capIndex++
    }
  }

  const newDataPoints = dataPoints.splice(0, capIndex)
  await storeNewDataPoints(connection, coin, newDataPoints)

  connection.end()
  return `Completed successfully`
}
