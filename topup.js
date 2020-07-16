const MySQLEvents = require('@rodrigogs/mysql-events');
var admin = require('firebase-admin');
// $env:GOOGLE_APPLICATION_CREDENTIALS="C:\wamp64\www\learnerapp\service-account.json"
let exchangeRate,exchangeRateIrators;
let paymentMethod = "M-Pesa";
let pool = require('./db_conn')
let separator = "------------------------------------------------------------------------------------------------";

admin.initializeApp({
    credential: admin.credential.cert({
        projectId: "learnerapp0001",
        clientEmail: "firebase-adminsdk-ktfuw@learnerapp0001.iam.gserviceaccount.com",
        privateKey: "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCtWVkRywNxxsYY\nRftNSzwygqpLqhZOogWA3783VyHqRusojUl3qmKN6IANFedl9AvvSCD1RTisUWhc\n0iRPhC488W5Whqi6WyFx5HlQrellk5sBXRbdlq3JL2C2FPcJKgF3zFh/5dvOTDb3\nJYl0DPmRLKipRhs1wtd0Kq1vrfqK7/sFHY7E13RnEkC63LLIAxnfT7IiVdb+/9mK\n69OjBHLpuehNEozfG0BLF0RBErm9KM+uPCPzlw7cJ+iPlU4S4DIH2IJBpZx3yZiT\nPUPlI/deQD34Qfi0Z1K0YBJcK8t7zWTgTzIQ9ddou0HwUe5w+BP9DppK55FYzYK3\nEMWShSAjAgMBAAECggEAETfOMoe9JFhvhaqxztgZtxSmcJSI2a+bJ1VSC1+itT/t\nDJ6X/QRV/K1qxqH11XkblF1QNLx26Bq/6HRQ+frxkskSQhLnAHebx1G/wQU9KQMz\nwrtp0YGtak3D5+IGNYlQEwOfiLiekAue7AZ09CtzwHk0mXemrTzYpKe2iWC5AYKx\nxw5A86Ql1n1ay9frk5B3AvgOVCMQGNIgS0o892289/2hkGLQrp00bs2IBh032LVH\nclmKYKIOK7LCarb7ukTBNuW4Luh8ZjrEooYSQuXVpX0G3Y4ZXEGhwP0jDbhD181D\nCDFioXHZE/6EUaDvDYxovP5cvEVxTqfL6zceEl4xmQKBgQDaF4l2GThB3OWX6mTs\nnHphyO2dZFnoiZo9358CSyxaK4HXUX7LFTydr/o1ZRAIeUhKVsfr4rPNtz7X4vsY\nx8JTlo+fAVukN+I8xihCxAoP6a3PeasSvpYOAifOH/1e78IM/UVMhbd7Zcs6K71O\nMlVdfw0zJj+IdK2DxkM5iWvvawKBgQDLeuEyagUxc+1o/424x58xCmfFkaqsI0/5\nQFlBM98UzSPFeVSYTRvYlQTrActcV1HB3GtmmlGsUgBwym+ZeCqmoZ7Qd6enj8A7\nwfbKOtZHhNOzZHU80eR1p1miSqQlt1vpVw6VYdpFXYEYiQJ6qxToV9qtI+b0fwLF\n21k5yI1YKQKBgCcce3ljcnRVUhNZZLoOIeBxcTN9sKYEL1YTSWfW5WBSVxmvMsbm\nyxUYXw2+Tw7F+VHjmDzUThyBVVLQEOnTwSTOZnlEfBPKNddiTgwTLh7GcHY5wpU8\n+poOhubvU7f31VwQ+6GKhWoqyjRnba6dVjPLOinHHTOygP+Vya6C14l7AoGBAKjl\nQBNWiST5MrCanoont+0+08/cDyx+yxz62psScTKU7AI3qY4ZQunNF53xiVkGaahe\nSw+JPA2qqw70GRnr8osJUAd9qj2dRlTTtQM7Py1yBT68PcvT9Kvr0qyxA/sCbVoL\nluFLrZ8x87vnzZUAeIQ6mBpq2INNAYI1haQ+4YOxAoGAOrCvHlzUNVzQtcIniR/M\n0307aCQCHvyqUcVXyDl3wTB0FAFpuN6pBQB/CZEmfrKny+ymMg1PoNPHEV3/I2/F\nZIokIozUnHAmDxDiYsGrv9YDdVo2YqSt2OzEQ2rBD8Ameh7iikNdyNi3v9PBb9nu\nsyzjPayL381+uZjU21LIVRY=\n-----END PRIVATE KEY-----\n"
      }),
//   databaseURL: "https://learnerapp-45257.firebaseio.com/"
    databaseURL: "https://learnerapp0001.firebaseio.com/"
});

var iratorsAdmin = admin.initializeApp({
    credential: admin.credential.cert({
        projectId: "iratorsapp0001",
        clientEmail: "firebase-adminsdk-yws4w@iratorsapp0001.iam.gserviceaccount.com",
        privateKey: "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC3ZonQ+T9btwEL\nA9nprCiIOT8nsL40pteZumf4wDWjpiZ7epN7bqADZeRk9WZrVQmOEofSoN/kha1s\nxCdntoZQBf+d8s3lbvaREcb438zXmM+w88Gf01jid9/XFCPxGifV23zwlfU6+F5S\nCzoeqciXaqOIEW6Jc+dNgDoX3OK3YH9O5Sh0AyQobS8IRxGddaIQS0vyDl3EFceB\nGBG8FpHsT0JS5L/m4cPdTC7jqCqX94MWH0QQidyQKEdc2udx4vIgMqr1ChVcOKi5\nrgmc+KlDztSQRXT3cjKGKzUcgtaCE3ISobp2sApERjSdbdh5s1Sl2jbvW3NP8DVA\nLIKpfzDfAgMBAAECggEAHL48xjFyDRk1gs5D4PFNhEADQQtcXAXJT3tdSn6Gnuhu\nKoxaQI46FMjreSkpYpP4GeSXQIWxtpp+xMefQgwN/neviUfMSMjRwDMFjewL/2l1\nz3lRyg8xDy6TYx1xyUUnThyGocbBdtBbKUeU1ckX5NHejEzobTFq5FoUK2OVGAEL\nMvA1ae2Y/RrBgC0REoRkHfMQ6Dh3Nq8eXDnLfBBF2d43Zp0aQAwZhuoRmWRtub9W\nHB4OiRy2yDZjPf4Ul111VqXgcN2g+jqegO3YQAwab+lLxYyKKkIzCJvj66EDf7s+\nYMLxnjcAEOIYM2n15PtB8e9v1Uqk2OnyixpGYmCVpQKBgQDdvMKn41NaJFe3esv/\nuz/TcttfsfYxg5UE9ZdIOcsFIvqKEzwFqLcSXgmqUqeYtlZLlL+hOtbzCbuY8ckN\nPorvjOXmyRxwJ4dDNkXQB3+70BXNNpIlz5HXa+ncplq5ZrYnnF7M76kjphUClXfD\nR3BEPxYyZHnHyLhJC+lr0hIepQKBgQDTvUsQ1t1NfFp1Y95DAcxctzRGT44B7VKl\nIDi3yHqvAggb0hi9cA1Rgn5RnkgtmU6BZfYP2UcPTbnn3xI0Dr7gDpTukvAjM26P\nkUO7RWHec5WC63pl3M7qBblL4Od+oyMD99JRxcVBkawOkFeWLzJYkIENZsicCEF+\nnqjRQxDeMwKBgQDD4Mn8UZ8CVHSAiyPG5308p4wPf0BDAUAVP3bCwPsNsJaufstZ\nHG+M9DGJmrae+wREhES8gyP7Uq+8LmszHfrSHx/Avgw3L1QYFcuaN+Wo3etEe16j\ntDfbm2LeHr6qZYeoekRsuZIrAb4xqCRCB8uvHiPXpFbIHBJfxPwQ2WRUWQKBgGsq\nuZ2SQnv/XoFfxJTBij+68hhMF6HeDiBJSKusKnv9WUFLev4Wgocotup0ZC/AEj3n\n7zxiSlbyjg9PlhUHCZC5kKOXdzc5xtGfQlq8aSZ/9cJHkLGRqqBDuV8wO7qasxRF\nEAwXPxlKs3zDjbETvjWZHdg8l3hxrUR65RLVEOqFAoGBAJkkFZcZn92mj94sOMJM\n+LSMWIvP+PQUyEIbAN1Ko9O68sfOCUujbPP+/EM/Nim02W7QglA0PsewnxrkXEuP\nQ60vvRHjzchOeMLC3wAIx0CbIDs/cAfhGyVNryYnQysFZribZwmX88RDrm+LD9OB\nnGkHoakFeCblQIBw23weuQIo\n-----END PRIVATE KEY-----\n"
      }),
    databaseURL: "https://iratorsapp0001.firebaseio.com/"
},'IratorsApp');

var ref = admin.database().ref();
var rateRef = ref.child('ExchangeRate');
rateRef.once('value',function(snapshot){
    exchangeRate = parseFloat(snapshot.val().exchange_rate);
    console.log("Exchange rate (L'earnerApp): "+exchangeRate);
});

var refIrators = iratorsAdmin.database().ref();
var rateRefIrators = refIrators.child('ExchangeRate');
rateRefIrators.once('value',function(snapshot){
    exchangeRateIrators = parseFloat(snapshot.val().exchange_rate);
    console.log("Exchange rate (IratorsApp): "+exchangeRateIrators);
});

console.log("Script initialised at: "+timeStamp());
console.log(separator);

const program = async () => {

  var instance = new MySQLEvents(pool, {
      startAtEnd: true,
      excludedSchemas: {
      mysql: true,
      },
  });

  await instance.start();

  pool.query('SELECT * FROM mpesa_payments', function (err, result, fields) {
    if (err) throw new Error(err)
    
    instance.addTrigger({
        name: 'LIVE',
        expression: 'moripesh_payments_live.mpesa_payments',
        statement: MySQLEvents.STATEMENTS.INSERT,
        onEvent: (event) => { // You will receive the events here

            var account = (event.affectedRows[0].after.BilRef);
            var appId = account.substring(0, 7);
            
            if (appId == 'irators'){
                console.log(separator);
                console.log("iratorsapp_transaction")
                var userRef = refIrators.child('Users').orderByChild('phone').equalTo('+'+event.affectedRows[0].after.MSISDN);
                userRef.once('value',function(snapshot){
                    if (!snapshot.exists){
                        console.log("phone number (MSISDN) "+ event.affectedRows[0].after.MSISDN +" does not exist in any user's account.")
                    } else {
                        const data = snapshot.val() || null;
                        if (data) {
                            let uid = Object.keys(data)[0];
                            
                            var emailRef = refIrators.child('Users').child(uid);
                            emailRef.once('value',function(snapshot){
                                let email = snapshot.val().email;
                                console.log(email);
                                var accountRef = refIrators.child('MonetaryAccount/'+uid);
                                accountRef.once('value',function(snapshot){
                                    if(!snapshot.exists || !snapshot.hasChild('current_balance')){
                                        accountRef.set({
                                            "current_balance": 0,
                                            "email": email,
                                            "previous_balance": 0,
                                            "status": "enabled"
                                        });
                                    }
                                    let oldBal = parseFloat(snapshot.val().current_balance);
                                    let amt = parseFloat((event.affectedRows[0].after.TransAmount*100)/exchangeRate+oldBal).toFixed(2);
                                    let newBal = amt;
                                    refIrators.child('MonetaryAccount/'+uid).update({
                                        "previous_balance": oldBal,
                                        "current_balance": newBal
                                    });
                                    console.log('Old Token Bal: '+oldBal+', New Token Bal: '+newBal);
                                    
                                    var transactionRef = refIrators.child('TokenPurchase/'+uid);
                                    transactionRef.push({
                                        "BalanceNew": newBal,
                                        "BalancePrevious": oldBal,
                                        "PaymentMethod": paymentMethod,
                                        "AccountNumber": '+'+event.affectedRows[0].after.MSISDN,
                                        "Time": timeStamp(),
                                        "TokensPurchased": parseFloat((event.affectedRows[0].after.TransAmount*100)/exchangeRate).toFixed(2),
                                        "TokensValue": parseFloat(event.affectedRows[0].after.TransAmount).toFixed(2),
                                        "User": uid,
                                        "RefNumber":event.affectedRows[0].after.TransID,
                                        "ExchangeRate":parseFloat(exchangeRate).toFixed(2)
                                    });
                                    console.log("Transaction Completed at: "+timeStamp());
                                    console.log(separator);
                                });
                            });
                        }
                    }
                });
            } else {
                console.log(separator);
                console.log("learnerapp_transaction")
                var userRef = ref.child('Users').orderByChild('phone').equalTo('+'+event.affectedRows[0].after.MSISDN);
                userRef.once('value',function(snapshot){
                    if (!snapshot.exists){
                        console.log("phone number (MSISDN) "+ event.affectedRows[0].after.MSISDN +" does not exist in any user's account.")
                    } else {
                        const data = snapshot.val() || null;
                        if (data) {
                            let uid = Object.keys(data)[0];
                            
                            var emailRef = ref.child('Users').child(uid);
                            emailRef.once('value',function(snapshot){
                                let email = snapshot.val().email;
                                console.log(email);
                                var accountRef = ref.child('MonetaryAccount/'+uid);
                                accountRef.once('value',function(snapshot){
                                    if(!snapshot.exists || !snapshot.hasChild('current_balance')){
                                        accountRef.set({
                                            "current_balance": 0,
                                            "email": email,
                                            "previous_balance": 0,
                                            "status": "enabled"
                                        });
                                    }
                                    let oldBal = parseFloat(snapshot.val().current_balance);
                                    let amt = parseFloat((event.affectedRows[0].after.TransAmount*100)/exchangeRate+oldBal).toFixed(2);
                                    let newBal = amt;
                                    ref.child('MonetaryAccount/'+uid).update({
                                        "previous_balance": oldBal,
                                        "current_balance": newBal
                                    });
                                    console.log('Old Token Bal: '+oldBal+', New Token Bal: '+newBal);
                                    
                                    var transactionRef = ref.child('TokenPurchase/'+uid);
                                    transactionRef.push({
                                        "BalanceNew": newBal,
                                        "BalancePrevious": oldBal,
                                        "PaymentMethod": paymentMethod,
                                        "AccountNumber": '+'+event.affectedRows[0].after.MSISDN,
                                        "Time": timeStamp(),
                                        "TokensPurchased": parseFloat((event.affectedRows[0].after.TransAmount*100)/exchangeRate).toFixed(2),
                                        "TokensValue": parseFloat(event.affectedRows[0].after.TransAmount).toFixed(2),
                                        "User": uid,
                                        "RefNumber":event.affectedRows[0].after.TransID,
                                        "ExchangeRate":parseFloat(exchangeRate).toFixed(2)
                                    });
                                    console.log("Transaction Completed at: "+timeStamp());
                                    console.log(separator);
                                });
                            });
                        }
                    }
                });
            }
            
        },
    });
    instance.on(MySQLEvents.EVENTS.CONNECTION_ERROR, console.error);
    instance.on(MySQLEvents.EVENTS.ZONGJI_ERROR, console.error);
  })
};
 
program()
  .then(() => console.log('Waiting for database events...'))
  .catch(console.error);

function timeStamp(){
  return new Date().toISOString()
  .replace(/T/, ' ')      // replace T with a space
  .replace(/\..+/, '');   // delete the dot and everything after
}