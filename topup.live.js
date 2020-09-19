const MySQLEvents = require('@rodrigogs/mysql-events');
var admin = require('firebase-admin');
let exchangeRate,exchangeRateIrators;
let paymentMethod = "M-Pesa";
let pool = require('./db_conn')
let separator = "------------------------------------------------------------------------------------------------";

admin.initializeApp({
    credential: admin.credential.cert({
        projectId: "learnerapp-45257",
        clientEmail: "learnerapp-45257@appspot.gserviceaccount.com",
        privateKey: "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDOblQnhYJGJ/4J\nwJ9E8RriC12HlZZxLbGtnt/Ie4yPuVT8YcHFCNnmSPBmyKIFotwQSlRB+Jmjqi/G\nctHrLoU6Iuf7bHPKVfUHrBrYjqPzhXKAxm4T2Ov9t33485kfFxVKVSg8tiAvqjGt\nDcWQS3CZXxn9se+lIlvEXDCkBcKI3NusvT3syN0M0PlZRsMYLbq8ojE6Gtpz16hT\nJuDwa4fPp0TpV62in72fxgl1Gmz9iutO55fCoZ0tXIkiBzC18OC1T9CJ9sNCvSWe\nBAhTxKSwRZdcRmr8cgtqyx/2qTfrVcHuW4BHGfL7N2So0rQkW22iTY+1VHCkvk6A\nHKVFbcgZAgMBAAECggEAKShJIgoV47v0g+hCOQIjPH9MmgxCjvUv0DB1aKjtaPho\nD9vDwO+XVjkjqTKGTz5dxuey7eZNZ5q2ZMgev1eqm2E7RF8mpOhHbsyrG5Mw8Awi\nvLcTWmStBIxxktGqrU6yYXwiBhy/xREXT2wIAFFyNToHWhQAdjmn9zn0PrehRhz6\nvY5k3zEkM4WYyD4l5vxlHa6Bife01Gx9L67hvhnCfJr5swA9yHUXKkjVv5HW5yuy\nOiCJlBiKVochL9IY32ZOe5f4NZcgKY88wnmy5kWlD4b1IY4MN4oDENfj50+Da4zH\noZSLDLRc4gT6lwE56i8oh5JFkLsjn+aFc3L8oG4xIwKBgQD5REjcHhSQTShrhUx6\n3OP15p8oX285E7ldobyDJIbZ70L/6lJQZEZ2DXvhy/VXVJwMAVylS0MxylIyhyxT\npYZkniyNQHffFFyWiw/1v+C2Z4THQmqNGcS4rFiJgwybdRRZDDu+THpTRfpwLRAU\nuWPFbLAmYM51tiW8bXrNiGCapwKBgQDUAdQhR3q9hv4Evs9N15QFWenGrDF1RWh3\nGtvYX+vWoXZvaduWnXYEzdb983c5l3H5h8ZqNePGOWhnzsc3+zQ6DCNBxHz8ZzS8\nJDcAaBdkh2ODJJ8HKg5rZ0YCyDjI9E6ceNUwDWJ5qZVhuq8PxToKaQdGTKm1jj/A\ntlrM4tafPwKBgGLVKVJldt/UR/+BxdY9OQGp5Fc8p7vozymJ1FXnKLTGgjk4LlUD\nvVBXflQD644p1QmJjLNZSRY30ymHoRK1YbkJYj7LXfwMdb2W+8gDwVRxbRsYgWja\n274hT6WOWXRWErBQAmwspJ/Z8jeCFosxCpcfxiQhZBsWEpZIjlVqCmunAoGAJP6c\nsaHzfRcNRG63ZfH0Vmq2LIDnsHsIG6CINF2ona9XJ2Hle4bGjVgN6AqQB3Cx5sUW\njnoQ4QoredHPWalwF6D+lf9ff/vSa/I11tBTJKurZbsCNRHVqlA/G0UtA4P+I7fC\nG7x6Lpi1BHS7D5lu41oods/x3UiRP2OSvKXopakCgYEA9sluBlyqUrz2GRTspuxT\nVJUwUP79xcNpyeJ8BcarqSOAaPkiPS5f+lY15pawaj0wDBWAqIo5rtq69GKY0/Ps\nMbzLhmcAUR4RGUz49Amv+ySWANwqpfSjMjxSbpt8tV5C9gclb5bXDsqzuixYENNc\ndCglmYqzILwt9t5zeFFmYIA=\n-----END PRIVATE KEY-----\n"
      }),
    databaseURL: "https://learnerapp-45257.firebaseio.com/"
});

var iratorsAdmin = admin.initializeApp({
    credential: admin.credential.cert({
        projectId: "iratorsapp",
        clientEmail: "firebase-adminsdk-6qih9@iratorsapp.iam.gserviceaccount.com",
        privateKey: "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDfDFkgEgskpMr2\n6CR2B7sww2R80KGJuQLwuB/PhQ3z67W7UR1cHxHNblbPxowVVbEsMwhF1NEbG/2+\nXDdBzbGkXvw+oLDhhxtiEw2WTOsNmsIlYs9w+Ucg9ngIdA0S0/dw9IOFeSCsHZ/5\n/oWHDkV7H4ZwG5yQq6JNpp3JO5Lee49L8IvXyi22JNJkMvsYrBO1oiXV6kRMcUuC\nRBeOhzzczdSrgy8w3TXNxFoYImRxwDqsufL2M7pO2zKmSzL9wAAZtvZ77RdxC0Yt\n1+pEiVqfqocR/Lu3wIw2iAy4wFQ31ltk9tw+IhNEbgabQKHQwjNU/u1M0sFMeO4B\nCyWcrAirAgMBAAECggEAGXswp2BIU5xb748fszg4414/GZ9ABRrUwilWwP9jU61m\n6opev0pxXzWuxzfYgwtyHSmLVWb1qJIvD9M7INJd6/hJ+uGis3Ea6L5ie/w3kitO\n6LR8HFzuCk798YFIiREcIJRefh4TZHZR815nZMF1oJY+ZopQ/ZoOBa72MaoTokTm\nxSMSyMwV6rRAzn+6FxSssWjsvQwNn7/cB34OjkORRr2vIzXFMeOJcAT1Yygn71Ow\ntanguJ914DrPMy1vNbLO2uHnWx5CfVfhSCMfe1RhR8tAkfPsSWEax2mDa8+s07Ak\nlxPKRpR6492/wHofFIJrKztBz7lNNIosIyUGA0QpAQKBgQDxZlanuNb6aePX8iUX\nCQt7pNQqvGds5sqX5pHYeAX3lCncpDyJ3uiJpfAcs+Le8Bva93VdEP65eM8lx1x/\nlaTx1QddXFF8opffRbL7xip5wTmJW8bmVRgPTQf39TqLsO7fd1Hct5Y1Dq/6/jtF\nVAf6YbGvav5n7It/RZc7zb10KwKBgQDsid4kaEwt5xJ1ep5M3jFfitnf86IAiZry\naHBJijVg+B5Hqqa/UXO4lSnYG/JG/F9E9RXdgv/rVu4DtSlcsI8nplkmrhn+dQ8G\ne6vnIHLB8+4oYIp8eWrDz1U+VLN6HGUzZfSNYMWabajb6C48kF8B8kuq2sgt0Zre\nU3e0DDL9gQKBgHOrkPw/IqvND4MLIWCfUeRGP+/WZUyWbh9JOTtbj6hpU4HJJMT/\njbdfTuXCAITI0uAiURduLFBdJg05MQZlgyrp4+SFdvpcwp8Wu8PO2c2Lm/FIi88U\nmTnDtHzJAeurtVpYx14WjiAQUJzZMzduI4CDTWv7vbm4a8NpaiUa2ZwPAoGBAJeB\nQwj8yPaJz/fxXx2LIAK7VAwu5/ACM33ayTlBTilbI0HyGzXlvQJYgGHMnKU9FuR7\nOtMUCkm591/mlKSq7jaORQISd9HVNpeHdTboQLjcPgocs/dAadMbpNT0ubSYJXYD\nct1vQ4JDhOm90Ie4TTJY+IBp+9flLpYCzmRDgBGBAoGBAIY6+6crlTOpblwlBaNX\nCen5dU+U1QylpgnEcZ90UdBEJaseG2yEOfS/PaBG/mZaSti5NfhUlBZi3QIDU8KS\n0gbVkGCLZWSDnHJ3SVnutlmd9xO8VnnWedVh1qUNFeRk3tv+Zg5J91RnRmfDLjKx\nTcOMWhgBnEXwPXQMCse5MVpr\n-----END PRIVATE KEY-----\n"
      }),
    databaseURL: "https://iratorsapp.firebaseio.com/"
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