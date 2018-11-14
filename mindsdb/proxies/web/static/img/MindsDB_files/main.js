var expanded = false;


var onReady = function(){
    hideInit();
    plotInit();
};

var hideInit = function() {
    $('.menu_nav').hide();
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
                targets: '#logo_img2',
                opacity: 1,

                complete: function(){
                    expanded = true;

                }
            });




            $('.menu_nav').css({'visibility':'visible'});
            $('.menu_nav').show();
            $('#email_input').focus();
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
        targets: '#logo_img2',
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