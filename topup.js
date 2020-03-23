const MySQLEvents = require('@rodrigogs/mysql-events');
var admin = require('firebase-admin');
// $env:GOOGLE_APPLICATION_CREDENTIALS="C:\wamp64\www\learnerapp\service-account.json"
let exchangeRate;
let paymentMethod = "M-Pesa";
let pool = require('./db_conn')

admin.initializeApp({
    credential: admin.credential.applicationDefault(),
//   databaseURL: "https://learnerapp-45257.firebaseio.com/"
    databaseURL: "https://learnerapp0001.firebaseio.com/"
});
var ref = admin.database().ref();
var rateRef = ref.child('ExchangeRate');
rateRef.once('value',function(snapshot){
    exchangeRate = parseFloat(snapshot.val().exchange_rate);
    console.log("Exchange rate: "+exchangeRate);
});
console.log("Script initialised at: "+timeStamp());
console.log("------------------------------------------------------------------------------------------------");

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
                                console.log("-------------------------------------------------------------------------------------------------------------------");
                            });
                        });
                    }
                }
            });
            
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