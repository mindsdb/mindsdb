rs.initiate({
    _id: 'replmain',
    members: [{_id: 0, host: '127.0.0.1:27001'}]
});
// we need create test_data db on exactly this shart.
// instance will think it is 'master' in 2-3 seconds after rs.initiate
sleep(5000);
use test_data;
db.placeholder.insert({x: 1});
