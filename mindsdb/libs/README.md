 * ```libs```: All mindsDB code
    * ```constants```: All mindsDB constants and structs
    * ```controllers```: The server controllers; which handle transaction requests
    * ```data_models/<framework>```: Here are the various model templates by framework, given the dynamic graph capabilities of pytorch we ship with only pytorch models, but support for tensorflow is provided
    * ```data_types```: These are MindsDB data types shared across modules
    * ```helpers```: These are the mindsDB collection of functions that can be used across modules
    * ```phases```: These are the modular phases involved in any given transaction in MindsDB
    * ```workers```: Since we can distribute train and test over a computing cloud, we place train and test as worker code that can run independently and in entirely different memory spaces.



