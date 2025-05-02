use std::collections::HashMap;


/// Users of this library must implement a setup() -> PSLRuntime function.
/// This function will be called by the PSL runtime to setup the runtime.

#[repr(C)]
pub struct RequestBody<'a> {
    /// URL, may contain query params
    pub path: &'a str,

    /// Body as text/json encoded in text
    pub body: &'a str,
}

#[repr(C)]
pub struct ResponseBody<'a> {
    /// HTTP Status Code
    pub status: i32,

    /// Body as text/json encoded in text
    pub body: &'a str,
}

pub type RequestHandler = extern "C" fn(
    request: *const RequestBody,
    response: *mut ResponseBody,
) -> i32;

#[repr(C)]
pub struct PSLRuntime {
    pub request_handlers: HashMap<String, RequestHandler>,
}
