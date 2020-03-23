//https://www.npmjs.com/package/@rodrigogs/mysql-events
var MySQLEvents = require('mysql-events');
var admin = require('firebase-admin');
let exchangeRate;
let paymentMethod = "M-Pesa";

admin.initializeApp({
    credential: admin.credential.applicationDefault(),
    databaseURL: "https://learnerapp0001.firebaseio.com"
});
var ref = admin.database().ref();
var rateRef = ref.child('ExchangeRate');
rateRef.once('value',function(snapshot){
    exchangeRate = parseFloat(snapshot.val().exchange_rate);
    console.log(exchangeRate);
});

var dsn = {
    host: "localhost",
    user: "binlogger",
    password: "admin1234"
};
var mysqlEventWatcher = MySQLEvents(dsn);
// console.log(mysqlEventWatcher);
var watcher = mysqlEventWatcher.add(
  'learnerapp.users',
  function (oldRow, newRow, event) {
     //row inserted
    if (oldRow === null) {
        console.log('row inserted');
        console.log(newRow.fields);
        var userRef = ref.child('Users').orderByChild('phone').equalTo('+'+newRow.fields.MSISDN);
        userRef.once('value',function(snapshot){
            // snapshot.forEach(function(userSnapshot){
            //     console.log('childSnapshot',userSnapshot.child('email').val());
            // })
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
                    let amt = parseFloat((newRow.fields.TransAmount)/exchangeRate+oldBal).toFixed(2);
                    let newBal = amt;
                    ref.child('MonetaryAccount/'+uid).update({
                        "previous_balance": oldBal,
                        "current_balance": newBal
                    });
                    console.log(oldBal+' '+newBal);
                    
                    var transactionRef = ref.child('TokenPurchase/'+uid);
                    transactionRef.push({
                        "BalanceNew": newBal,
                        "BalancePrevious": oldBal,
                        "PaymentMethod": paymentMethod,
                        "AccountNumber": '+'+newRow.fields.MSISDN,
                        "Time": timeStamp(),
                        "TokensPurchased": parseFloat((newRow.fields.TransAmount)/exchangeRate).toFixed(2),
                        "TokensValue": parseFloat(newRow.fields.TransAmount).toFixed(2),
                        "User": uid,
                        "RefNumber":"12345",
                        "ExchangeRate":parseFloat(exchangeRate).toFixed(2)
                    });
                    console.log("transaction complete");
                });
              });
            }
        });
    }
 
     //row deleted
    if (newRow === null) {
        console.log('row deleted');
        console.log(oldRow.fields);
    }
 
     //row updated
    if (oldRow !== null && newRow !== null) {
        console.log('row updated');
        console.log(newRow.fields);
        var userRef = ref.child('Users').orderByChild('phone').equalTo('+'+newRow.fields.MSISDN);
        userRef.once('value',function(snapshot){
            // snapshot.forEach(function(userSnapshot){
            //     console.log('childSnapshot',userSnapshot.child('email').val());
            // })
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
                    let amt = parseFloat((newRow.fields.TransAmount)/exchangeRate+oldBal).toFixed(2);
                    let newBal = amt;
                    ref.child('MonetaryAccount/'+uid).update({
                        "previous_balance": oldBal,
                        "current_balance": newBal
                    });
                    console.log(oldBal+' '+newBal);
                    
                    var transactionRef = ref.child('TokenPurchase/'+uid);
                    transactionRef.push({
                        "BalanceNew": newBal,
                        "BalancePrevious": oldBal,
                        "PaymentMethod": paymentMethod,
                        "AccountNumber": '+'+newRow.fields.MSISDN,
                        "Time": timeStamp(),
                        "TokensPurchased": parseFloat((newRow.fields.TransAmount)/exchangeRate).toFixed(2),
                        "TokensValue": parseFloat(newRow.fields.TransAmount).toFixed(2),
                        "User": uid,
                        "RefNumber":"12345",
                        "ExchangeRate":parseFloat(exchangeRate).toFixed(2)
                    });
                    console.log("transaction complete");
                });
              });
            }
        });
    }
 
    //detailed event information
    // console.log(event)
  }, 
  'Active'
);

function timeStamp(){
    return new Date().toISOString()
    .replace(/T/, ' ')      // replace T with a space
    .replace(/\..+/, '');   // delete the dot and everything after
}