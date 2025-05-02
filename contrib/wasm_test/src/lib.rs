use std::collections::HashMap;

use runtime::{PSLRuntime, RequestHandler};


pub extern "C" fn hello_world(
    request: *const runtime::RequestBody,
    response: *mut runtime::ResponseBody,
) -> i32 {
    let request = unsafe { &*request };
    let response = unsafe { &mut *response };

    // Set the response status and body
    response.status = 200;
    response.body = "Hello, World!";

    0 // Return 0 to indicate success
}

pub extern "C" fn setup() -> PSLRuntime {
    let mut runtime = PSLRuntime {
        request_handlers: HashMap::new(),
    };

    // Register the request handlers
    runtime.request_handlers.insert(
        "/hello".to_string(),
        hello_world as RequestHandler,
    );

    runtime
}