'use strict';

// Rebound
// =======
// **Rebound** is a simple library that models Spring dynamics for the
// purpose of driving physical animations.
//
// Origin
// ------
// [Rebound](http://facebook.github.io/rebound) was originally written
// in Java to provide a lightweight physics system for
// [Home](https://play.google.com/store/apps/details?id=com.facebook.home) and
// [Chat Heads](https://play.google.com/store/apps/details?id=com.facebook.orca)
// on Android. It's now been adopted by several other Android
// applications. This JavaScript port was written to provide a quick
// way to demonstrate Rebound animations on the web for a
// [conference talk](https://www.youtube.com/watch?v=s5kNm-DgyjY). Since then
// the JavaScript version has been used to build some really nice interfaces.
// Check out [brandonwalkin.com](http://brandonwalkin.com) for an
// example.
//
// Overview
// --------
// The Library provides a SpringSystem for maintaining a set of Spring
// objects and iterating those Springs through a physics solver loop
// until equilibrium is achieved. The Spring class is the basic
// animation driver provided by Rebound. By attaching a listener to
// a Spring, you can observe its motion. The observer function is
// notified of position changes on the spring as it solves for
// equilibrium. These position updates can be mapped to an animation
// range to drive animated property updates on your user interface
// elements (translation, rotation, scale, etc).
//
// Example
// -------
// Here's a simple example. Pressing and releasing on the logo below
// will cause it to scale up and down with a springy animation.
//
// <div style="text-align:center; margin-bottom:50px; margin-top:50px">
//   <img
//     src="http://facebook.github.io/rebound/images/rebound.png"
//     id="logo"
//   />
// </div>
// <script src="../rebound.min.js"></script>
// <script>
//
// function scale(el, val) {
//   el.style.mozTransform =
//   el.style.msTransform =
//   el.style.webkitTransform =
//   el.style.transform = 'scale3d(' + val + ', ' + val + ', 1)';
// }
// var el = document.getElementById('logo');
//
// var springSystem = new rebound.SpringSystem();
// var spring = springSystem.createSpring(50, 3);
// spring.addListener({
//   onSpringUpdate: function(spring) {
//     var val = spring.getCurrentValue();
//     val = rebound.MathUtil.mapValueInRange(val, 0, 1, 1, 0.5);
//     scale(el, val);
//   }
// });
//
// el.addEventListener('mousedown', function() {
//   spring.setEndValue(1);
// });
//
// el.addEventListener('mouseout', function() {
//   spring.setEndValue(0);
// });
//
// el.addEventListener('mouseup', function() {
//   spring.setEndValue(0);
// });
//
// </script>
//
// Here's how it works.
//
// ```
// // Get a reference to the logo element.
// var el = document.getElementById('logo');
//
// // create a SpringSystem and a Spring with a bouncy config.
// var springSystem = new rebound.SpringSystem();
// var spring = springSystem.createSpring(50, 3);
//
// // Add a listener to the spring. Every time the physics
// // solver updates the Spring's value onSpringUpdate will
// // be called.
// spring.addListener({
//   onSpringUpdate: function(spring) {
//     var val = spring.getCurrentValue();
//     val = rebound.MathUtil
//                  .mapValueInRange(val, 0, 1, 1, 0.5);
//     scale(el, val);
//   }
// });
//
// // Listen for mouse down/up/out and toggle the
// //springs endValue from 0 to 1.
// el.addEventListener('mousedown', function() {
//   spring.setEndValue(1);
// });
//
// el.addEventListener('mouseout', function() {
//   spring.setEndValue(0);
// });
//
// el.addEventListener('mouseup', function() {
//   spring.setEndValue(0);
// });
//
// // Helper for scaling an element with css transforms.
// function scale(el, val) {
//   el.style.mozTransform =
//   el.style.msTransform =
//   el.style.webkitTransform =
//   el.style.transform = 'scale3d(' +
//     val + ', ' + val + ', 1)';
// }
// ```

(function () {
  var rebound = {};
  var util = rebound.util = {};
  var concat = Array.prototype.concat;
  var slice = Array.prototype.slice;

  // Bind a function to a context object.
  util.bind = function bind(func, context) {
    var args = slice.call(arguments, 2);
    return function () {
      func.apply(context, concat.call(args, slice.call(arguments)));
    };
  };

  // Add all the properties in the source to the target.
  util.extend = function extend(target, source) {
    for (var key in source) {
      if (source.hasOwnProperty(key)) {
        target[key] = source[key];
      }
    }
  };

  // SpringSystem
  // ------------
  // **SpringSystem** is a set of Springs that all run on the same physics
  // timing loop. To get started with a Rebound animation you first
  // create a new SpringSystem and then add springs to it.
  var SpringSystem = rebound.SpringSystem = function SpringSystem(looper) {
    this._springRegistry = {};
    this._activeSprings = [];
    this.listeners = [];
    this._idleSpringIndices = [];
    this.looper = looper || new AnimationLooper();
    this.looper.springSystem = this;
  };

  util.extend(SpringSystem.prototype, {

    _springRegistry: null,

    _isIdle: true,

    _lastTimeMillis: -1,

    _activeSprings: null,

    listeners: null,

    _idleSpringIndices: null,

    // A SpringSystem is iterated by a looper. The looper is responsible
    // for executing each frame as the SpringSystem is resolved to idle.
    // There are three types of Loopers described below AnimationLooper,
    // SimulationLooper, and SteppingSimulationLooper. AnimationLooper is
    // the default as it is the most useful for common UI animations.
    setLooper: function setLooper(looper) {
      this.looper = looper;
      looper.springSystem = this;
    },

    // Add a new spring to this SpringSystem. This Spring will now be solved for
    // during the physics iteration loop. By default the spring will use the
    // default Origami spring config with 40 tension and 7 friction, but you can
    // also provide your own values here.
    createSpring: function createSpring(tension, friction) {
      var springConfig;
      if (tension === undefined || friction === undefined) {
        springConfig = SpringConfig.DEFAULT_ORIGAMI_SPRING_CONFIG;
      } else {
        springConfig = SpringConfig.fromOrigamiTensionAndFriction(tension, friction);
      }
      return this.createSpringWithConfig(springConfig);
    },

    // Add a spring with a specified bounciness and speed. To replicate Origami
    // compositions based on PopAnimation patches, use this factory method to
    // create matching springs.
    createSpringWithBouncinessAndSpeed: function createSpringWithBouncinessAndSpeed(bounciness, speed) {
      var springConfig;
      if (bounciness === undefined || speed === undefined) {
        springConfig = SpringConfig.DEFAULT_ORIGAMI_SPRING_CONFIG;
      } else {
        springConfig = SpringConfig.fromBouncinessAndSpeed(bounciness, speed);
      }
      return this.createSpringWithConfig(springConfig);
    },

    // Add a spring with the provided SpringConfig.
    createSpringWithConfig: function createSpringWithConfig(springConfig) {
      var spring = new Spring(this);
      this.registerSpring(spring);
      spring.setSpringConfig(springConfig);
      return spring;
    },

    // You can check if a SpringSystem is idle or active by calling
    // getIsIdle. If all of the Springs in the SpringSystem are at rest,
    // i.e. the physics forces have reached equilibrium, then this
    // method will return true.
    getIsIdle: function getIsIdle() {
      return this._isIdle;
    },

    // Retrieve a specific Spring from the SpringSystem by id. This
    // can be useful for inspecting the state of a spring before
    // or after an integration loop in the SpringSystem executes.
    getSpringById: function getSpringById(id) {
      return this._springRegistry[id];
    },

    // Get a listing of all the springs registered with this
    // SpringSystem.
    getAllSprings: function getAllSprings() {
      var vals = [];
      for (var id in this._springRegistry) {
        if (this._springRegistry.hasOwnProperty(id)) {
          vals.push(this._springRegistry[id]);
        }
      }
      return vals;
    },

    // registerSpring is called automatically as soon as you create
    // a Spring with SpringSystem#createSpring. This method sets the
    // spring up in the registry so that it can be solved in the
    // solver loop.
    registerSpring: function registerSpring(spring) {
      this._springRegistry[spring.getId()] = spring;
    },

    // Deregister a spring with this SpringSystem. The SpringSystem will
    // no longer consider this Spring during its integration loop once
    // this is called. This is normally done automatically for you when
    // you call Spring#destroy.
    deregisterSpring: function deregisterSpring(spring) {
      removeFirst(this._activeSprings, spring);
      delete this._springRegistry[spring.getId()];
    },

    advance: function advance(time, deltaTime) {
      while (this._idleSpringIndices.length > 0) {
        this._idleSpringIndices.pop();
      }for (var i = 0, len = this._activeSprings.length; i < len; i++) {
        var spring = this._activeSprings[i];
        if (spring.systemShouldAdvance()) {
          spring.advance(time / 1000.0, deltaTime / 1000.0);
        } else {
          this._idleSpringIndices.push(this._activeSprings.indexOf(spring));
        }
      }
      while (this._idleSpringIndices.length > 0) {
        var idx = this._idleSpringIndices.pop();
        idx >= 0 && this._activeSprings.splice(idx, 1);
      }
    },

    // This is our main solver loop called to move the simulation
    // forward through time. Before each pass in the solver loop
    // onBeforeIntegrate is called on an any listeners that have
    // registered themeselves with the SpringSystem. This gives you
    // an opportunity to apply any constraints or adjustments to
    // the springs that should be enforced before each iteration
    // loop. Next the advance method is called to move each Spring in
    // the systemShouldAdvance forward to the current time. After the
    // integration step runs in advance, onAfterIntegrate is called
    // on any listeners that have registered themselves with the
    // SpringSystem. This gives you an opportunity to run any post
    // integration constraints or adjustments on the Springs in the
    // SpringSystem.
    loop: function loop(currentTimeMillis) {
      var listener;
      if (this._lastTimeMillis === -1) {
        this._lastTimeMillis = currentTimeMillis - 1;
      }
      var ellapsedMillis = currentTimeMillis - this._lastTimeMillis;
      this._lastTimeMillis = currentTimeMillis;

      var i = 0,
          len = this.listeners.length;
      for (i = 0; i < len; i++) {
        listener = this.listeners[i];
        listener.onBeforeIntegrate && listener.onBeforeIntegrate(this);
      }

      this.advance(currentTimeMillis, ellapsedMillis);
      if (this._activeSprings.length === 0) {
        this._isIdle = true;
        this._lastTimeMillis = -1;
      }

      for (i = 0; i < len; i++) {
        listener = this.listeners[i];
        listener.onAfterIntegrate && listener.onAfterIntegrate(this);
      }

      if (!this._isIdle) {
        this.looper.run();
      }
    },

    // activateSpring is used to notify the SpringSystem that a Spring
    // has become displaced. The system responds by starting its solver
    // loop up if it is currently idle.
    activateSpring: function activateSpring(springId) {
      var spring = this._springRegistry[springId];
      if (this._activeSprings.indexOf(spring) == -1) {
        this._activeSprings.push(spring);
      }
      if (this.getIsIdle()) {
        this._isIdle = false;
        this.looper.run();
      }
    },

    // Add a listener to the SpringSystem so that you can receive
    // before/after integration notifications allowing Springs to be
    // constrained or adjusted.
    addListener: function addListener(listener) {
      this.listeners.push(listener);
    },

    // Remove a previously added listener on the SpringSystem.
    removeListener: function removeListener(listener) {
      removeFirst(this.listeners, listener);
    },

    // Remove all previously added listeners on the SpringSystem.
    removeAllListeners: function removeAllListeners() {
      this.listeners = [];
    }

  });

  // Spring
  // ------
  // **Spring** provides a model of a classical spring acting to
  // resolve a body to equilibrium. Springs have configurable
  // tension which is a force multipler on the displacement of the
  // spring from its rest point or `endValue` as defined by [Hooke's
  // law](http://en.wikipedia.org/wiki/Hooke's_law). Springs also have
  // configurable friction, which ensures that they do not oscillate
  // infinitely. When a Spring is displaced by updating it's resting
  // or `currentValue`, the SpringSystems that contain that Spring
  // will automatically start looping to solve for equilibrium. As each
  // timestep passes, `SpringListener` objects attached to the Spring
  // will be notified of the updates providing a way to drive an
  // animation off of the spring's resolution curve.
  var Spring = rebound.Spring = function Spring(springSystem) {
    this._id = 's' + Spring._ID++;
    this._springSystem = springSystem;
    this.listeners = [];
    this._currentState = new PhysicsState();
    this._previousState = new PhysicsState();
    this._tempState = new PhysicsState();
  };

  util.extend(Spring, {
    _ID: 0,

    MAX_DELTA_TIME_SEC: 0.064,

    SOLVER_TIMESTEP_SEC: 0.001

  });

  util.extend(Spring.prototype, {

    _id: 0,

    _springConfig: null,

    _overshootClampingEnabled: false,

    _currentState: null,

    _previousState: null,

    _tempState: null,

    _startValue: 0,

    _endValue: 0,

    _wasAtRest: true,

    _restSpeedThreshold: 0.001,

    _displacementFromRestThreshold: 0.001,

    listeners: null,

    _timeAccumulator: 0,

    _springSystem: null,

    // Remove a Spring from simulation and clear its listeners.
    destroy: function destroy() {
      this.listeners = [];
      this.frames = [];
      this._springSystem.deregisterSpring(this);
    },

    // Get the id of the spring, which can be used to retrieve it from
    // the SpringSystems it participates in later.
    getId: function getId() {
      return this._id;
    },

    // Set the configuration values for this Spring. A SpringConfig
    // contains the tension and friction values used to solve for the
    // equilibrium of the Spring in the physics loop.
    setSpringConfig: function setSpringConfig(springConfig) {
      this._springConfig = springConfig;
      return this;
    },

    // Retrieve the SpringConfig used by this Spring.
    getSpringConfig: function getSpringConfig() {
      return this._springConfig;
    },

    // Set the current position of this Spring. Listeners will be updated
    // with this value immediately. If the rest or `endValue` is not
    // updated to match this value, then the spring will be dispalced and
    // the SpringSystem will start to loop to restore the spring to the
    // `endValue`.
    //
    // A common pattern is to move a Spring around without animation by
    // calling.
    //
    // ```
    // spring.setCurrentValue(n).setAtRest();
    // ```
    //
    // This moves the Spring to a new position `n`, sets the endValue
    // to `n`, and removes any velocity from the `Spring`. By doing
    // this you can allow the `SpringListener` to manage the position
    // of UI elements attached to the spring even when moving without
    // animation. For example, when dragging an element you can
    // update the position of an attached view through a spring
    // by calling `spring.setCurrentValue(x)`. When
    // the gesture ends you can update the Springs
    // velocity and endValue
    // `spring.setVelocity(gestureEndVelocity).setEndValue(flingTarget)`
    // to cause it to naturally animate the UI element to the resting
    // position taking into account existing velocity. The codepaths for
    // synchronous movement and spring driven animation can
    // be unified using this technique.
    setCurrentValue: function setCurrentValue(currentValue, skipSetAtRest) {
      this._startValue = currentValue;
      this._currentState.position = currentValue;
      if (!skipSetAtRest) {
        this.setAtRest();
      }
      this.notifyPositionUpdated(false, false);
      return this;
    },

    // Get the position that the most recent animation started at. This
    // can be useful for determining the number off oscillations that
    // have occurred.
    getStartValue: function getStartValue() {
      return this._startValue;
    },

    // Retrieve the current value of the Spring.
    getCurrentValue: function getCurrentValue() {
      return this._currentState.position;
    },

    // Get the absolute distance of the Spring from it's resting endValue
    // position.
    getCurrentDisplacementDistance: function getCurrentDisplacementDistance() {
      return this.getDisplacementDistanceForState(this._currentState);
    },

    getDisplacementDistanceForState: function getDisplacementDistanceForState(state) {
      return Math.abs(this._endValue - state.position);
    },

    // Set the endValue or resting position of the spring. If this
    // value is different than the current value, the SpringSystem will
    // be notified and will begin running its solver loop to resolve
    // the Spring to equilibrium. Any listeners that are registered
    // for onSpringEndStateChange will also be notified of this update
    // immediately.
    setEndValue: function setEndValue(endValue) {
      if (this._endValue == endValue && this.isAtRest()) {
        return this;
      }
      this._startValue = this.getCurrentValue();
      this._endValue = endValue;
      this._springSystem.activateSpring(this.getId());
      for (var i = 0, len = this.listeners.length; i < len; i++) {
        var listener = this.listeners[i];
        var onChange = listener.onSpringEndStateChange;
        onChange && onChange(this);
      }
      return this;
    },

    // Retrieve the endValue or resting position of this spring.
    getEndValue: function getEndValue() {
      return this._endValue;
    },

    // Set the current velocity of the Spring. As previously mentioned,
    // this can be useful when you are performing a direct manipulation
    // gesture. When a UI element is released you may call setVelocity
    // on its animation Spring so that the Spring continues with the
    // same velocity as the gesture ended with. The friction, tension,
    // and displacement of the Spring will then govern its motion to
    // return to rest on a natural feeling curve.
    setVelocity: function setVelocity(velocity) {
      if (velocity === this._currentState.velocity) {
        return this;
      }
      this._currentState.velocity = velocity;
      this._springSystem.activateSpring(this.getId());
      return this;
    },

    // Get the current velocity of the Spring.
    getVelocity: function getVelocity() {
      return this._currentState.velocity;
    },

    // Set a threshold value for the movement speed of the Spring below
    // which it will be considered to be not moving or resting.
    setRestSpeedThreshold: function setRestSpeedThreshold(restSpeedThreshold) {
      this._restSpeedThreshold = restSpeedThreshold;
      return this;
    },

    // Retrieve the rest speed threshold for this Spring.
    getRestSpeedThreshold: function getRestSpeedThreshold() {
      return this._restSpeedThreshold;
    },

    // Set a threshold value for displacement below which the Spring
    // will be considered to be not displaced i.e. at its resting
    // `endValue`.
    setRestDisplacementThreshold: function setRestDisplacementThreshold(displacementFromRestThreshold) {
      this._displacementFromRestThreshold = displacementFromRestThreshold;
    },

    // Retrieve the rest displacement threshold for this spring.
    getRestDisplacementThreshold: function getRestDisplacementThreshold() {
      return this._displacementFromRestThreshold;
    },

    // Enable overshoot clamping. This means that the Spring will stop
    // immediately when it reaches its resting position regardless of
    // any existing momentum it may have. This can be useful for certain
    // types of animations that should not oscillate such as a scale
    // down to 0 or alpha fade.
    setOvershootClampingEnabled: function setOvershootClampingEnabled(enabled) {
      this._overshootClampingEnabled = enabled;
      return this;
    },

    // Check if overshoot clamping is enabled for this spring.
    isOvershootClampingEnabled: function isOvershootClampingEnabled() {
      return this._overshootClampingEnabled;
    },

    // Check if the Spring has gone past its end point by comparing
    // the direction it was moving in when it started to the current
    // position and end value.
    isOvershooting: function isOvershooting() {
      var start = this._startValue;
      var end = this._endValue;
      return this._springConfig.tension > 0 && (start < end && this.getCurrentValue() > end || start > end && this.getCurrentValue() < end);
    },

    // Spring.advance is the main solver method for the Spring. It takes
    // the current time and delta since the last time step and performs
    // an RK4 integration to get the new position and velocity state
    // for the Spring based on the tension, friction, velocity, and
    // displacement of the Spring.
    advance: function advance(time, realDeltaTime) {
      var isAtRest = this.isAtRest();

      if (isAtRest && this._wasAtRest) {
        return;
      }

      var adjustedDeltaTime = realDeltaTime;
      if (realDeltaTime > Spring.MAX_DELTA_TIME_SEC) {
        adjustedDeltaTime = Spring.MAX_DELTA_TIME_SEC;
      }

      this._timeAccumulator += adjustedDeltaTime;

      var tension = this._springConfig.tension,
          friction = this._springConfig.friction,
          position = this._currentState.position,
          velocity = this._currentState.velocity,
          tempPosition = this._tempState.position,
          tempVelocity = this._tempState.velocity,
          aVelocity,
          aAcceleration,
          bVelocity,
          bAcceleration,
          cVelocity,
          cAcceleration,
          dVelocity,
          dAcceleration,
          dxdt,
          dvdt;

      while (this._timeAccumulator >= Spring.SOLVER_TIMESTEP_SEC) {

        this._timeAccumulator -= Spring.SOLVER_TIMESTEP_SEC;

        if (this._timeAccumulator < Spring.SOLVER_TIMESTEP_SEC) {
          this._previousState.position = position;
          this._previousState.velocity = velocity;
        }

        aVelocity = velocity;
        aAcceleration = tension * (this._endValue - tempPosition) - friction * velocity;

        tempPosition = position + aVelocity * Spring.SOLVER_TIMESTEP_SEC * 0.5;
        tempVelocity = velocity + aAcceleration * Spring.SOLVER_TIMESTEP_SEC * 0.5;
        bVelocity = tempVelocity;
        bAcceleration = tension * (this._endValue - tempPosition) - friction * tempVelocity;

        tempPosition = position + bVelocity * Spring.SOLVER_TIMESTEP_SEC * 0.5;
        tempVelocity = velocity + bAcceleration * Spring.SOLVER_TIMESTEP_SEC * 0.5;
        cVelocity = tempVelocity;
        cAcceleration = tension * (this._endValue - tempPosition) - friction * tempVelocity;

        tempPosition = position + cVelocity * Spring.SOLVER_TIMESTEP_SEC * 0.5;
        tempVelocity = velocity + cAcceleration * Spring.SOLVER_TIMESTEP_SEC * 0.5;
        dVelocity = tempVelocity;
        dAcceleration = tension * (this._endValue - tempPosition) - friction * tempVelocity;

        dxdt = 1.0 / 6.0 * (aVelocity + 2.0 * (bVelocity + cVelocity) + dVelocity);
        dvdt = 1.0 / 6.0 * (aAcceleration + 2.0 * (bAcceleration + cAcceleration) + dAcceleration);

        position += dxdt * Spring.SOLVER_TIMESTEP_SEC;
        velocity += dvdt * Spring.SOLVER_TIMESTEP_SEC;
      }

      this._tempState.position = tempPosition;
      this._tempState.velocity = tempVelocity;

      this._currentState.position = position;
      this._currentState.velocity = velocity;

      if (this._timeAccumulator > 0) {
        this._interpolate(this._timeAccumulator / Spring.SOLVER_TIMESTEP_SEC);
      }

      if (this.isAtRest() || this._overshootClampingEnabled && this.isOvershooting()) {

        if (this._springConfig.tension > 0) {
          this._startValue = this._endValue;
          this._currentState.position = this._endValue;
        } else {
          this._endValue = this._currentState.position;
          this._startValue = this._endValue;
        }
        this.setVelocity(0);
        isAtRest = true;
      }

      var notifyActivate = false;
      if (this._wasAtRest) {
        this._wasAtRest = false;
        notifyActivate = true;
      }

      var notifyAtRest = false;
      if (isAtRest) {
        this._wasAtRest = true;
        notifyAtRest = true;
      }

      this.notifyPositionUpdated(notifyActivate, notifyAtRest);
    },

    notifyPositionUpdated: function notifyPositionUpdated(notifyActivate, notifyAtRest) {
      for (var i = 0, len = this.listeners.length; i < len; i++) {
        var listener = this.listeners[i];
        if (notifyActivate && listener.onSpringActivate) {
          listener.onSpringActivate(this);
        }

        if (listener.onSpringUpdate) {
          listener.onSpringUpdate(this);
        }

        if (notifyAtRest && listener.onSpringAtRest) {
          listener.onSpringAtRest(this);
        }
      }
    },

    // Check if the SpringSystem should advance. Springs are advanced
    // a final frame after they reach equilibrium to ensure that the
    // currentValue is exactly the requested endValue regardless of the
    // displacement threshold.
    systemShouldAdvance: function systemShouldAdvance() {
      return !this.isAtRest() || !this.wasAtRest();
    },

    wasAtRest: function wasAtRest() {
      return this._wasAtRest;
    },

    // Check if the Spring is atRest meaning that it's currentValue and
    // endValue are the same and that it has no velocity. The previously
    // described thresholds for speed and displacement define the bounds
    // of this equivalence check. If the Spring has 0 tension, then it will
    // be considered at rest whenever its absolute velocity drops below the
    // restSpeedThreshold.
    isAtRest: function isAtRest() {
      return Math.abs(this._currentState.velocity) < this._restSpeedThreshold && (this.getDisplacementDistanceForState(this._currentState) <= this._displacementFromRestThreshold || this._springConfig.tension === 0);
    },

    // Force the spring to be at rest at its current position. As
    // described in the documentation for setCurrentValue, this method
    // makes it easy to do synchronous non-animated updates to ui
    // elements that are attached to springs via SpringListeners.
    setAtRest: function setAtRest() {
      this._endValue = this._currentState.position;
      this._tempState.position = this._currentState.position;
      this._currentState.velocity = 0;
      return this;
    },

    _interpolate: function _interpolate(alpha) {
      this._currentState.position = this._currentState.position * alpha + this._previousState.position * (1 - alpha);
      this._currentState.velocity = this._currentState.velocity * alpha + this._previousState.velocity * (1 - alpha);
    },

    getListeners: function getListeners() {
      return this.listeners;
    },

    addListener: function addListener(newListener) {
      this.listeners.push(newListener);
      return this;
    },

    removeListener: function removeListener(listenerToRemove) {
      removeFirst(this.listeners, listenerToRemove);
      return this;
    },

    removeAllListeners: function removeAllListeners() {
      this.listeners = [];
      return this;
    },

    currentValueIsApproximately: function currentValueIsApproximately(value) {
      return Math.abs(this.getCurrentValue() - value) <= this.getRestDisplacementThreshold();
    }

  });

  // PhysicsState
  // ------------
  // **PhysicsState** consists of a position and velocity. A Spring uses
  // this internally to keep track of its current and prior position and
  // velocity values.
  var PhysicsState = function PhysicsState() {};

  util.extend(PhysicsState.prototype, {
    position: 0,
    velocity: 0
  });

  // SpringConfig
  // ------------
  // **SpringConfig** maintains a set of tension and friction constants
  // for a Spring. You can use fromOrigamiTensionAndFriction to convert
  // values from the [Origami](http://facebook.github.io/origami/)
  // design tool directly to Rebound spring constants.
  var SpringConfig = rebound.SpringConfig = function SpringConfig(tension, friction) {
    this.tension = tension;
    this.friction = friction;
  };

  // Loopers
  // -------
  // **AnimationLooper** plays each frame of the SpringSystem on animation
  // timing loop. This is the default type of looper for a new spring system
  // as it is the most common when developing UI.
  var AnimationLooper = rebound.AnimationLooper = function AnimationLooper() {
    this.springSystem = null;
    var _this = this;
    var _run = function _run() {
      _this.springSystem.loop(Date.now());
    };

    this.run = function () {
      util.onFrame(_run);
    };
  };

  // **SimulationLooper** resolves the SpringSystem to a resting state in a
  // tight and blocking loop. This is useful for synchronously generating
  // pre-recorded animations that can then be played on a timing loop later.
  // Sometimes this lead to better performance to pre-record a single spring
  // curve and use it to drive many animations; however, it can make dynamic
  // response to user input a bit trickier to implement.
  rebound.SimulationLooper = function SimulationLooper(timestep) {
    this.springSystem = null;
    var time = 0;
    var running = false;
    timestep = timestep || 16.667;

    this.run = function () {
      if (running) {
        return;
      }
      running = true;
      while (!this.springSystem.getIsIdle()) {
        this.springSystem.loop(time += timestep);
      }
      running = false;
    };
  };

  // **SteppingSimulationLooper** resolves the SpringSystem one step at a
  // time controlled by an outside loop. This is useful for testing and
  // verifying the behavior of a SpringSystem or if you want to control your own
  // timing loop for some reason e.g. slowing down or speeding up the
  // simulation.
  rebound.SteppingSimulationLooper = function (timestep) {
    this.springSystem = null;
    var time = 0;

    // this.run is NOOP'd here to allow control from the outside using
    // this.step.
    this.run = function () {};

    // Perform one step toward resolving the SpringSystem.
    this.step = function (timestep) {
      this.springSystem.loop(time += timestep);
    };
  };

  // Math for converting from
  // [Origami](http://facebook.github.io/origami/) to
  // [Rebound](http://facebook.github.io/rebound).
  // You mostly don't need to worry about this, just use
  // SpringConfig.fromOrigamiTensionAndFriction(v, v);
  var OrigamiValueConverter = rebound.OrigamiValueConverter = {
    tensionFromOrigamiValue: function tensionFromOrigamiValue(oValue) {
      return (oValue - 30.0) * 3.62 + 194.0;
    },

    origamiValueFromTension: function origamiValueFromTension(tension) {
      return (tension - 194.0) / 3.62 + 30.0;
    },

    frictionFromOrigamiValue: function frictionFromOrigamiValue(oValue) {
      return (oValue - 8.0) * 3.0 + 25.0;
    },

    origamiFromFriction: function origamiFromFriction(friction) {
      return (friction - 25.0) / 3.0 + 8.0;
    }
  };

  // BouncyConversion provides math for converting from Origami PopAnimation
  // config values to regular Origami tension and friction values. If you are
  // trying to replicate prototypes made with PopAnimation patches in Origami,
  // then you should create your springs with
  // SpringSystem.createSpringWithBouncinessAndSpeed, which uses this Math
  // internally to create a spring to match the provided PopAnimation
  // configuration from Origami.
  var BouncyConversion = rebound.BouncyConversion = function (bounciness, speed) {
    this.bounciness = bounciness;
    this.speed = speed;
    var b = this.normalize(bounciness / 1.7, 0, 20.0);
    b = this.projectNormal(b, 0.0, 0.8);
    var s = this.normalize(speed / 1.7, 0, 20.0);
    this.bouncyTension = this.projectNormal(s, 0.5, 200);
    this.bouncyFriction = this.quadraticOutInterpolation(b, this.b3Nobounce(this.bouncyTension), 0.01);
  };

  util.extend(BouncyConversion.prototype, {

    normalize: function normalize(value, startValue, endValue) {
      return (value - startValue) / (endValue - startValue);
    },

    projectNormal: function projectNormal(n, start, end) {
      return start + n * (end - start);
    },

    linearInterpolation: function linearInterpolation(t, start, end) {
      return t * end + (1.0 - t) * start;
    },

    quadraticOutInterpolation: function quadraticOutInterpolation(t, start, end) {
      return this.linearInterpolation(2 * t - t * t, start, end);
    },

    b3Friction1: function b3Friction1(x) {
      return 0.0007 * Math.pow(x, 3) - 0.031 * Math.pow(x, 2) + 0.64 * x + 1.28;
    },

    b3Friction2: function b3Friction2(x) {
      return 0.000044 * Math.pow(x, 3) - 0.006 * Math.pow(x, 2) + 0.36 * x + 2.;
    },

    b3Friction3: function b3Friction3(x) {
      return 0.00000045 * Math.pow(x, 3) - 0.000332 * Math.pow(x, 2) + 0.1078 * x + 5.84;
    },

    b3Nobounce: function b3Nobounce(tension) {
      var friction = 0;
      if (tension <= 18) {
        friction = this.b3Friction1(tension);
      } else if (tension > 18 && tension <= 44) {
        friction = this.b3Friction2(tension);
      } else {
        friction = this.b3Friction3(tension);
      }
      return friction;
    }
  });

  util.extend(SpringConfig, {
    // Convert an origami Spring tension and friction to Rebound spring
    // constants. If you are prototyping a design with Origami, this
    // makes it easy to make your springs behave exactly the same in
    // Rebound.
    fromOrigamiTensionAndFriction: function fromOrigamiTensionAndFriction(tension, friction) {
      return new SpringConfig(OrigamiValueConverter.tensionFromOrigamiValue(tension), OrigamiValueConverter.frictionFromOrigamiValue(friction));
    },

    // Convert an origami PopAnimation Spring bounciness and speed to Rebound
    // spring constants. If you are using PopAnimation patches in Origami, this
    // utility will provide springs that match your prototype.
    fromBouncinessAndSpeed: function fromBouncinessAndSpeed(bounciness, speed) {
      var bouncyConversion = new rebound.BouncyConversion(bounciness, speed);
      return this.fromOrigamiTensionAndFriction(bouncyConversion.bouncyTension, bouncyConversion.bouncyFriction);
    },

    // Create a SpringConfig with no tension or a coasting spring with some
    // amount of Friction so that it does not coast infininitely.
    coastingConfigWithOrigamiFriction: function coastingConfigWithOrigamiFriction(friction) {
      return new SpringConfig(0, OrigamiValueConverter.frictionFromOrigamiValue(friction));
    }
  });

  SpringConfig.DEFAULT_ORIGAMI_SPRING_CONFIG = SpringConfig.fromOrigamiTensionAndFriction(40, 7);

  util.extend(SpringConfig.prototype, { friction: 0, tension: 0 });

  // Here are a couple of function to convert colors between hex codes and RGB
  // component values. These are handy when performing color
  // tweening animations.
  var colorCache = {};
  util.hexToRGB = function (color) {
    if (colorCache[color]) {
      return colorCache[color];
    }
    color = color.replace('#', '');
    if (color.length === 3) {
      color = color[0] + color[0] + color[1] + color[1] + color[2] + color[2];
    }
    var parts = color.match(/.{2}/g);

    var ret = {
      r: parseInt(parts[0], 16),
      g: parseInt(parts[1], 16),
      b: parseInt(parts[2], 16)
    };

    colorCache[color] = ret;
    return ret;
  };

  util.rgbToHex = function (r, g, b) {
    r = r.toString(16);
    g = g.toString(16);
    b = b.toString(16);
    r = r.length < 2 ? '0' + r : r;
    g = g.length < 2 ? '0' + g : g;
    b = b.length < 2 ? '0' + b : b;
    return '#' + r + g + b;
  };

  var MathUtil = rebound.MathUtil = {
    // This helper function does a linear interpolation of a value from
    // one range to another. This can be very useful for converting the
    // motion of a Spring to a range of UI property values. For example a
    // spring moving from position 0 to 1 could be interpolated to move a
    // view from pixel 300 to 350 and scale it from 0.5 to 1. The current
    // position of the `Spring` just needs to be run through this method
    // taking its input range in the _from_ parameters with the property
    // animation range in the _to_ parameters.
    mapValueInRange: function mapValueInRange(value, fromLow, fromHigh, toLow, toHigh) {
      var fromRangeSize = fromHigh - fromLow;
      var toRangeSize = toHigh - toLow;
      var valueScale = (value - fromLow) / fromRangeSize;
      return toLow + valueScale * toRangeSize;
    },

    // Interpolate two hex colors in a 0 - 1 range or optionally provide a
    // custom range with fromLow,fromHight. The output will be in hex by default
    // unless asRGB is true in which case it will be returned as an rgb string.
    interpolateColor: function interpolateColor(val, startColor, endColor, fromLow, fromHigh, asRGB) {
      fromLow = fromLow === undefined ? 0 : fromLow;
      fromHigh = fromHigh === undefined ? 1 : fromHigh;
      startColor = util.hexToRGB(startColor);
      endColor = util.hexToRGB(endColor);
      var r = Math.floor(util.mapValueInRange(val, fromLow, fromHigh, startColor.r, endColor.r));
      var g = Math.floor(util.mapValueInRange(val, fromLow, fromHigh, startColor.g, endColor.g));
      var b = Math.floor(util.mapValueInRange(val, fromLow, fromHigh, startColor.b, endColor.b));
      if (asRGB) {
        return 'rgb(' + r + ',' + g + ',' + b + ')';
      } else {
        return util.rgbToHex(r, g, b);
      }
    },

    degreesToRadians: function degreesToRadians(deg) {
      return deg * Math.PI / 180;
    },

    radiansToDegrees: function radiansToDegrees(rad) {
      return rad * 180 / Math.PI;
    }

  };

  util.extend(util, MathUtil);

  // Utilities
  // ---------
  // Here are a few useful JavaScript utilities.

  // Lop off the first occurence of the reference in the Array.
  function removeFirst(array, item) {
    var idx = array.indexOf(item);
    idx != -1 && array.splice(idx, 1);
  }

  var _onFrame;
  if (typeof window !== 'undefined') {
    _onFrame = window.requestAnimationFrame || window.webkitRequestAnimationFrame || window.mozRequestAnimationFrame || window.msRequestAnimationFrame || window.oRequestAnimationFrame || function (callback) {
      window.setTimeout(callback, 1000 / 60);
    };
  }
  if (!_onFrame && typeof process !== 'undefined' && process.title === 'node') {
    _onFrame = setImmediate;
  }

  // Cross browser/node timer functions.
  util.onFrame = function onFrame(func) {
    return _onFrame(func);
  };

  // Export the public api using exports for common js or the window for
  // normal browser inclusion.
  if (typeof exports != 'undefined') {
    util.extend(exports, rebound);
  } else if (typeof window != 'undefined') {
    window.rebound = rebound;
  }
})();

// Legal Stuff
// -----------
/**
 *  Copyright (c) 2013, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 */
'use strict';

/**
 * Polygon.
 * Create a regular polygon and provide api to compute inscribed child.
 */

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var Polygon = function () {
  function Polygon() {
    var radius = arguments.length <= 0 || arguments[0] === undefined ? 100 : arguments[0];
    var sides = arguments.length <= 1 || arguments[1] === undefined ? 3 : arguments[1];
    var depth = arguments.length <= 2 || arguments[2] === undefined ? 0 : arguments[2];
    var colors = arguments[3];

    _classCallCheck(this, Polygon);

    this._radius = radius;
    this._sides = sides;
    this._depth = depth;
    this._colors = colors;

    this._x = 0;
    this._y = 0;

    this.rotation = 0;
    this.scale = 1;

    // Get basePolygon points straight away.
    this.points = this._getRegularPolygonPoints();
  }

  /**
   * Get the points of any regular polygon based on
   * the number of sides and radius.
   */


  _createClass(Polygon, [{
    key: '_getRegularPolygonPoints',
    value: function _getRegularPolygonPoints() {

      var points = [];

      var i = 0;

      while (i < this._sides) {
        // Note that sin and cos are inverted in order to draw
        // polygon pointing down like: ∇
        var x = -this._radius * Math.sin(i * 2 * Math.PI / this._sides);
        var y = this._radius * Math.cos(i * 2 * Math.PI / this._sides);

        points.push({ x: x, y: y });

        i++;
      }

      return points;
    }

    /**
     * Get the inscribed polygon points by calling `getInterpolatedPoint`
     * for the points (start, end) of each side.
     */

  }, {
    key: '_getInscribedPoints',
    value: function _getInscribedPoints(points, progress) {
      var _this = this;

      var inscribedPoints = [];

      points.forEach(function (item, i) {

        var start = item;
        var end = points[i + 1];

        if (!end) {
          end = points[0];
        }

        var point = _this._getInterpolatedPoint(start, end, progress);

        inscribedPoints.push(point);
      });

      return inscribedPoints;
    }

    /**
     * Get interpolated point using linear interpolation
     * on x and y axis.
     */

  }, {
    key: '_getInterpolatedPoint',
    value: function _getInterpolatedPoint(start, end, progress) {

      var Ax = start.x;
      var Ay = start.y;

      var Bx = end.x;
      var By = end.y;

      // Linear interpolation formula:
      // point = start + (end - start) * progress;
      var Cx = Ax + (Bx - Ax) * progress;
      var Cy = Ay + (By - Ay) * progress;

      return {
        x: Cx,
        y: Cy
      };
    }

    /**
     * Update children points array.
     */

  }, {
    key: '_getUpdatedChildren',
    value: function _getUpdatedChildren(progress) {

      var children = [];

      for (var i = 0; i < this._depth; i++) {

        // Get basePolygon points on first lap
        // then get previous child points.
        var points = children[i - 1] || this.points;

        var inscribedPoints = this._getInscribedPoints(points, progress);

        children.push(inscribedPoints);
      }

      return children;
    }

    /**
     * Render children, first update children array,
     * then loop and draw each child.
     */

  }, {
    key: 'renderChildren',
    value: function renderChildren(context, progress) {
      var _this2 = this;

      var children = this._getUpdatedChildren(progress);

      // child = array of points at a certain progress over the parent sides.
      children.forEach(function (points, i) {

        // Draw child.
        context.beginPath();
        points.forEach(function (point) {
          return context.lineTo(point.x, point.y);
        });
        context.closePath();

        // Set colors.
        var strokeColor = _this2._colors.stroke;
        var childColor = _this2._colors.child;

        if (strokeColor) {
          context.strokeStyle = strokeColor;
          context.stroke();
        }

        if (childColor) {
          var rgb = rebound.util.hexToRGB(childColor);

          var alphaUnit = 1 / children.length;
          var alpha = alphaUnit + alphaUnit * i;

          var rgba = 'rgba(' + rgb.r + ', ' + rgb.g + ', ' + rgb.b + ', ' + alpha + ')';

          context.fillStyle = rgba;

          // Set Shadow.
          context.shadowColor = 'rgba(0,0,0, 0.1)';
          context.shadowBlur = 10;
          context.shadowOffsetX = 0;
          context.shadowOffsetY = 0;

          context.fill();
        }
      });
    }

    /**
     * Render.
     */

  }, {
    key: 'render',
    value: function render(context) {

      context.save();

      context.translate(this._x, this._y);

      if (this.rotation !== 0) {
        context.rotate(rebound.MathUtil.degreesToRadians(this.rotation));
      }

      if (this.scale !== 1) {
        context.scale(this.scale, this.scale);
      }

      // Draw basePolygon.
      context.beginPath();
      this.points.forEach(function (point) {
        return context.lineTo(point.x, point.y);
      });
      context.closePath();

      // Set colors.
      var strokeColor = this._colors.stroke;
      var childColor = this._colors.base;

      if (strokeColor) {
        context.strokeStyle = strokeColor;
        context.stroke();
      }

      if (childColor) {
        context.fillStyle = childColor;
        context.fill();
      }

      context.restore();
    }
  }]);

  return Polygon;
}();
'use strict';

/**
 * Spinner.
 * Create a canvas element, append it to the body, render polygon with
 * inscribed children, provide init and complete methods to control spinner.
 */

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var Spinner = function () {
  function Spinner(params) {
    _classCallCheck(this, Spinner);

    var id = params.id,
        radius = params.radius,
        sides = params.sides,
        depth = params.depth,
        colors = params.colors,
        alwaysForward = params.alwaysForward,
        restAt = params.restAt,
        renderBase = params.renderBase;

    if (sides < 3) {
      console.warn('At least 3 sides required.');
      sides = 3;
    }

    this._canvas = document.createElement('canvas');
    this._canvas.style.backgroundColor = colors.background;

    this._canvasW = null;
    this._canvasH = null;
    this._canvasOpacity = 1;

    this._centerX = null;
    this._centerY = null;

    this._alwaysForward = alwaysForward;
    this._restThreshold = restAt;
    this._renderBase = renderBase;

    this._springRangeLow = 0;
    this._springRangeHigh = this._restThreshold || 1;

    // Instantiate basePolygon.
    this._basePolygon = new Polygon(radius, sides, depth, colors);

    this._progress = 0;

    this._isAutoSpin = null;
    this._isCompleting = null;
  }

  /**
   * Init spinner.
   */


  _createClass(Spinner, [{
    key: 'init',
    value: function init(spring, autoSpin) {

      this._addCanvas();

      this._spring = spring;
      this._addSpringListener();

      this._isAutoSpin = autoSpin;

      if (autoSpin) {
        // Start auto spin.
        this._spin();
      } else {
        // Render first frame only.
        this._spring.setEndValue(0);
        this.render();
      }
    }
  }, {
    key: '_addCanvas',
    value: function _addCanvas() {
      document.body.appendChild(this._canvas);
      this._context = this._canvas.getContext('2d');
      this._setCanvasSize();
    }
  }, {
    key: '_setCanvasSize',
    value: function _setCanvasSize() {
      this._canvasW = this._canvas.width = window.innerWidth;
      this._canvasH = this._canvas.height = window.innerHeight;

      this._canvas.style.position = 'fixed';
      this._canvas.style.top = 0;
      this._canvas.style.left = 0;

      this._centerX = this._canvasW / 2;
      this._centerY = this._canvasH / 2;
    }
  }, {
    key: '_addSpringListener',
    value: function _addSpringListener() {

      var ctx = this;

      // Add a listener to the spring. Every time the physics
      // solver updates the Spring's value onSpringUpdate will
      // be called.
      this._spring.addListener({
        onSpringUpdate: function onSpringUpdate(spring) {

          var val = spring.getCurrentValue();

          // Input range in the `from` parameters.
          var fromLow = 0,
              fromHigh = 1,

          // Property animation range in the `to` parameters.
          toLow = ctx._springRangeLow,
              toHigh = ctx._springRangeHigh;

          val = rebound.MathUtil.mapValueInRange(val, fromLow, fromHigh, toLow, toHigh);

          // Note that the render method is
          // called with the spring motion value.
          ctx.render(val);
        }
      });
    }

    /**
     * Start complete animation.
     */

  }, {
    key: 'setComplete',
    value: function setComplete() {
      this._isCompleting = true;
    }
  }, {
    key: '_completeAnimation',
    value: function _completeAnimation() {

      // Fade out the canvas.
      this._canvasOpacity -= 0.1;
      this._canvas.style.opacity = this._canvasOpacity;

      // Stop animation and remove canvas.
      if (this._canvasOpacity <= 0) {
        this._isAutoSpin = false;
        this._spring.setAtRest();
        this._canvas.remove();
      }
    }

    /**
     * Spin animation.
     */

  }, {
    key: '_spin',
    value: function _spin() {

      if (this._alwaysForward) {

        var currentValue = this._spring.getCurrentValue();

        // Switch the animation range used to compute the value
        // in the `onSpringUpdate`, so to change the reverse animation
        // of the spring at a certain threshold.
        if (this._restThreshold && currentValue === 1) {
          this._switchSpringRange();
        }

        // In order to keep the motion going forward
        // when spring reach 1 reset to 0 at rest.
        if (currentValue === 1) {
          this._spring.setCurrentValue(0).setAtRest();
        }
      }

      // Restart the spinner.
      this._spring.setEndValue(this._spring.getCurrentValue() === 1 ? 0 : 1);
    }
  }, {
    key: '_switchSpringRange',
    value: function _switchSpringRange() {

      var threshold = this._restThreshold;

      this._springRangeLow = this._springRangeLow === threshold ? 0 : threshold;
      this._springRangeHigh = this._springRangeHigh === threshold ? 1 : threshold;
    }

    /**
     * Render.
     */

  }, {
    key: 'render',
    value: function render(progress) {

      // Update progess if present and round to 4th decimal.
      if (progress) {
        this._progress = Math.round(progress * 10000) / 10000;
      }

      // Restart the spin.
      if (this._isAutoSpin && this._spring.isAtRest()) {
        this._spin();
      }

      // Complete the animation.
      if (this._isCompleting) {
        this._completeAnimation();
      }

      // Clear canvas and save context.
      this._context.clearRect(0, 0, this._canvasW, this._canvasH);
      this._context.save();

      // Move to center.
      this._context.translate(this._centerX, this._centerY);

      this._context.lineWidth = 1.5;

      // Render basePolygon.
      if (this._renderBase) {
        this._basePolygon.render(this._context);
      }

      // Render inscribed polygons.
      this._basePolygon.renderChildren(this._context, this._progress);

      this._context.restore();
    }
  }]);

  return Spinner;
}();
'use strict';

// Custom SETTINGS for each demo in related index.html

var settings = SETTINGS || {
  rebound: {
    tension: 2,
    friction: 5
  },
  spinner: {
    radius: 80,
    sides: 3,
    depth: 4,
    colors: {
      background: '#000000',
      stroke: '#000000',
      base: '#222222',
      child: '#FFFFFF'
    },
    alwaysForward: true, // When false the spring will reverse normally.
    restAt: 0.5, // A number from 0.1 to 0.9 || null for full rotation
    renderBase: true // Optionally render basePolygon
  }
};

/**
 * Demo.
 */
var demo = {
  settings: settings,

  spring: null,
  spinner: null,

  /**
   * Initialize Rebound.js with settings.
   * Rebound is used to generate a spring which
   * is then used to animate the spinner.
   * See more: http://facebook.github.io/rebound-js/docs/rebound.html
   */
  initRebound: function initRebound() {

    var settings = demo.settings.rebound;

    // Create a SpringSystem.
    var springSystem = new rebound.SpringSystem();

    // Add a spring to the system.
    demo.spring = springSystem.createSpring(settings.tension, settings.friction);
  },


  /**
   * Initialize Spinner with settings.
   */
  initSpinner: function initSpinner() {

    var settings = demo.settings.spinner;

    // Instantiate Spinner.
    demo.spinner = new Spinner(settings);
  },


  /**
   * Initialize demo.
   */
  init: function init(callback) {

    var spinnerTypeAutoSpin = true;

    // Instantiate animation engine and spinner system.
    demo.initRebound();
    demo.initSpinner();

    // Init animation with Rebound Spring System.
    demo.spinner.init(demo.spring, spinnerTypeAutoSpin);

    if (spinnerTypeAutoSpin) {
      // Fake loading time, in a real world just call demo.spinner.setComplete();
      // whenever the preload will be completed.
      setTimeout(function () {
        demo.spinner.setComplete();
        callback();
      }, 6000);
    } else {
      // Perform real ajax request.
      demo.loadSomething();
    }
  },


  /**
   * Ajax Request.
   */
  loadSomething: function loadSomething() {

    var oReq = new XMLHttpRequest();

    oReq.addEventListener('progress', function (oEvent) {
      if (oEvent.lengthComputable) {

        var percent = Math.ceil(oEvent.loaded / oEvent.total * 100);
        console.log('ajax loding percent', percent);

        // By setting the end value with the actual loading percentage
        // the spinner will progress based on the actual ajax loading time.
        demo.spring.setEndValue(percent * 0.01);
      }
    });

    oReq.addEventListener('load', function (e) {
      // Complete the loading animation.
      demo.spinner.setComplete();
    });

    oReq.open('GET', '/img/something.jpg');
    oReq.send();
  }
};
//# sourceMappingURL=main.js.map
