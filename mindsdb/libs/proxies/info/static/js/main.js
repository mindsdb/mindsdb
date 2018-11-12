var expanded = true;


function validateEmail(email) {
  var re = /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
  return re.test(email);
}


var initSocket = function() {
    mdbsocket = io.connect(MINDSDB_CONFIG.LOGGING_WEBSOCKET_URL);

    mdbsocket.resp_calls = {};

    mdbsocket.registerResponse = function(uid, callback){
        mdbsocket.resp_calls[uid] = callback;
    };

    mdbsocket.on('call_response', function(payload){
        var uid = payload.uid;
        var response = payload.response;
        var error = payload.error;

        error = (typeof error !== 'undefined') ?  error : false;

        if(error != false){
            console.error(error);
        }
        else{
            if(uid in mdbsocket.resp_calls){
                mdbsocket.resp_calls[uid](response);
                delete mdbsocket.resp_calls[uid];
            }
            else{
                console.info('callback for response not defined, not a bad thing, but logging it, just in case, call-uid:'+uid);
            }
        }
    });


    mdbsocket.callService = function(service, data, callback) {
        callback = (typeof callback !== 'undefined') ?  callback : false;

        var uid = _.uniqueId('uid_');
        if(callback!=false){
            mdbsocket.registerResponse(uid, callback);
        }
        mdbsocket.emit('call', {service:service, data:data, uid:uid});
    };

    mdbsocket.subscribeToCollection = function(collection, filter, callback) {
        var uid = _.uniqueId('uid_');
        mdbsocket.emit('collection', {collection:collection, filter:filter, uid:uid});
    };
};

var showInitScreen=function(){

    $('#body_cover').show();
    $('#page_body').show();
    plotInit();
    $('#page_body').hide();
    $('.hide_on_start').hide();

    var bg_selecttor = $('#logo_img_black_bg');
    bg_selecttor.hide();
    bg_selecttor.css({opacity:0, left:'20%'});
    bg_selecttor.show();

    anime({
        targets: '#logo_img_black_bg',
        top:'48%',
        opacity: 1
    });

    return {
        showServerHelp : function(){
            $('#body_cover').hide();
            $('#give_us_email').hide();
            $('#server_help').show();
        },
        showEmailInput: function(email) {
            $('#body_cover').hide();
            $('#server_help').hide();
            $('#give_us_email').show();
            $('#email_input').focus();
            var email = (typeof email !== 'undefined')? email:  $('#email_input').val();
            if (email == false) return;
            $('#email_input').val(email);

        },
        showLogsScreen: function() {
            $('#body_cover').hide();
            $('.hide_on_start').hide();
            $('#give_us_email').show();
            $('#terms_conditions').hide();
            $('#page_body').show();

            anime({
                targets: '#logo_img_black_bg',
                top:'0%',
                opacity: 0,
                complete: function(){
                    bg_selecttor.css({opacity:0, left:'0%', top:'0%'});
                    bg_selecttor.show();
                }
            });

            expandMenu();
        },
        renderMlModels: function(ml_models) {

        }
    }

};


var current_state =  false;


var onReady = function(){

    var init_state = showInitScreen();
    current_state = init_state;

    initSocket();

    mdbsocket.on('connect', function() {

        mdbsocket.callService('getUserEmail', false, function(email){

            var email = (typeof email !== 'undefined')? email: false;

            if(!email){
                init_state.showEmailInput();
            }
            else {
                init_state.showEmailInput(email);
                init_state.showLogsScreen();
            }
        });
    });


    mdbsocket.on('connect_error', function(){
        init_state.showServerHelp();
    });

    mdbsocket.subscribeToCollection('ml_models', {}, function(ml_models) {
        init_state.renderMlModels(ml_models);
    });

    mdbsocket.subscribeToCollection('models_data', {}, function(models_data) {
        init_state.renderMlModelsData(models_data);
    });

};





var continueToLogs = function(email){

    var email = (typeof email !== 'undefined')? email:  $('#email_input').val();
    if (email == false) return;

    $('#email_input').val(email);

    var is_valid = validateEmail(email);

    if (is_valid) {
        mdbsocket.callService('setUserEmail', {email: email});

    }
    else {
        $('#email_not_valid').show();
        setTimeout(function(){$('#email_not_valid').hide();}, 1900);
    }



};



var plotInit = function() {
    var chart = c3.generate({
        bindto: '#chart',
        data: {
            xs: {
                rental_price: 'rental_price_x'

            },
            // iris data from R
            columns: [
                ["rental_price_x", 3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1, 3.7, 3.4, 3.0, 3.0, 4.0, 4.4, 3.9, 3.5, 3.8, 3.8, 3.4, 3.7, 3.6, 3.3, 3.4, 3.0, 3.4, 3.5, 3.4, 3.2, 3.1, 3.4, 4.1, 4.2, 3.1, 3.2, 3.5, 3.6, 3.0, 3.4, 3.5, 2.3, 3.2, 3.5, 3.8, 3.0, 3.8, 3.2, 3.7, 3.3,
                3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1, 3.7, 3.4, 3.0, 3.0, 4.0, 4.4, 3.9, 3.5, 3.8, 3.8, 3.4, 3.7, 3.6, 3.3, 3.4, 3.0, 3.4, 3.5, 3.4, 3.2, 3.1, 3.4, 4.1, 4.2, 3.1, 3.2, 3.5, 3.6, 3.0, 3.4, 3.5, 2.3, 3.2, 3.5, 3.8, 3.0, 3.8, 3.2, 3.7, 3.3,
                3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1, 3.7, 3.4, 3.0, 3.0, 4.0, 4.4, 3.9, 3.5, 3.8, 3.8, 3.4, 3.7, 3.6, 3.3, 3.4, 3.0, 3.4, 3.5, 3.4, 3.2, 3.1, 3.4, 4.1, 4.2, 3.1, 3.2, 3.5, 3.6, 3.0, 3.4, 3.5, 2.3, 3.2, 3.5, 3.8, 3.0, 3.8, 3.2, 3.7, 3.3],
                ["rental_price", 0.2, 0.2, 0.2, 0.2, 0.2, 0.4, 0.3, 0.2, 0.2, 0.1, 0.2, 0.2, 0.1, 0.1, 0.2, 0.4, 0.4, 0.3, 0.3, 0.3, 0.2, 0.4, 0.2, 0.5, 0.2, 0.2, 0.4, 0.2, 0.2, 0.2, 0.2, 0.4, 0.1, 0.2, 0.2, 0.2, 0.2, 0.1, 0.2, 0.2, 0.3, 0.3, 0.2, 0.6, 0.4, 0.3, 0.2, 0.2, 0.2, 0.2,
                3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1, 3.7, 3.4, 3.0, 3.0, 4.0, 4.4, 3.9, 3.5, 3.8, 3.8, 3.4, 3.7, 3.6, 3.3, 3.4, 3.0, 3.4, 3.5, 3.4, 3.2, 3.1, 3.4, 4.1, 4.2, 3.1, 3.2, 3.5, 3.6, 3.0, 3.4, 3.5, 2.3, 3.2, 3.5, 3.8, 3.0, 3.8, 3.2, 3.7, 3.3,
                3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1, 3.7, 3.4, 3.0, 3.0, 4.0, 4.4, 3.9, 3.5, 3.8, 3.8, 3.4, 3.7, 3.6, 3.3, 3.4, 3.0, 3.4, 3.5, 3.4, 3.2, 3.1, 3.4, 4.1, 4.2, 3.1, 3.2, 3.5, 3.6, 3.0, 3.4, 3.5, 2.3, 3.2, 3.5, 3.8, 3.0, 3.8, 3.2, 3.7, 3.3]
            ],
            colors: {
                rental_price: '#2ab673',
            },
            type: 'scatter'
        },
        axis: {
            x: {
                label: 'Real',
                tick: {
                    fit: false
                }
            },
            y: {
                label: 'Predicted'
            }
        }
    });
};


var expandMenu = function() {


    expanded = -1;

    anime({
        targets: '.bothbgs',
        fill: "#2ab673",
        color: "#2ab673",
        easing: 'easeInOutExpo',

    });

    anime({
        targets: '#page_body',
        translateX: "-20%",
        scale: 0.5,
        opacity: 0.9,
        "border-radius": "5px",
        "complete": function () {



            anime({
                targets: '#logo_img',
                opacity: 0
            });

            anime({
                targets: '#logo_img_black_bg',
                opacity: 1,

                complete: function(){
                    expanded = true;

                }
            });


            $('.menu_nav').css({'visibility':'visible'});
            $('.menu_nav').show();

            anime({
              targets: '.itemss',
              translateX: 20,
                'z-index': 1,
              direction: 'alternate',
              loop: false,
              duration: function(el, i, l) {
                return 1000 + (i * 1000);
              }
            });

        }
    });


};

var collapseMenu = function() {

    expanded = -1;


    anime({
        targets: '#logo_img_black_bg',
        opacity: 0
    });

    anime({
                targets: '#logo_img',
                opacity: 1,


            });

    anime({
                targets: '.bothbgs',
                fill: "rgb(0, 0, 0)",
                color: "rgb(0, 0, 0)",
                easing: 'easeInOutExpo',
                "z-index": 10,

                complete: function(){
                    expanded = false;
                }

            });

    anime({
        targets: '#page_body',
        translateX: "0%",
        scale: 1,
        opacity: 1,
        "border-radius": "0px",
        "complete": function () {

            anime({
                targets: '.bothbgs2',
                "background-color": "#181818",
                easing: 'easeInOutExpo',

            });

        }
    });


};




var menuToggle = function(){
    if (expanded == -1) return;
    if (expanded == false) expandMenu();
    else collapseMenu();
}


$(document).ready(onReady);