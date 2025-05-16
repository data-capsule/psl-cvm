use bytes::{BufMut, BytesMut};

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

impl Default for ResponseBody<'_> {
    fn default() -> Self {
        Self {
            status: 0,
            body: "",
        }
    }
}

pub type RequestHandler = extern "C" fn(
    request: *const RequestBody,
    response: *mut ResponseBody,
) -> i32;

#[repr(C)]
pub struct PSLRuntime<'a> {
    pub num_handlers: u8,
    pub request_url_paths: &'a [&'a str],
    pub request_function_names: &'a [&'a str],
}

impl<'a> PSLRuntime<'a> {
    pub fn new() -> Self {
        Self {
            num_handlers: 0,
            request_url_paths: &[],
            request_function_names: &[],
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        buf.put_u8(self.num_handlers);

        for &path in self.request_url_paths {
            let path_bytes = path.as_bytes();
            buf.put_u8(path_bytes.len() as u8);
            buf.put_slice(path_bytes);
        }

        for &name in self.request_function_names {
            let name_bytes = name.as_bytes();
            buf.put_u8(name_bytes.len() as u8);
            buf.put_slice(name_bytes);
        }

        buf.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, pin::Pin};

    use crate::{PSLRuntime, RequestBody, ResponseBody};


    #[test]
    fn load() {

        // Need to make lib live as long as the end of the program

        let (func_assoc, _lib) = unsafe {
            let lib = libloading::Library::new("../contrib/target/debug/libwasm_test.dylib").unwrap();

            let setup: libloading::Symbol<extern "C" fn() -> Pin<Box<PSLRuntime<'static>>>> =
                lib.get(b"setup").unwrap();

            let runtime = setup();
            assert_eq!(runtime.num_handlers, 1);

            let mut func_assoc = HashMap::new();

            for i in 0..runtime.num_handlers {
                let path = runtime.request_url_paths[i as usize];
                let name = runtime.request_function_names[i as usize];
                let func: libloading::Symbol<extern "C" fn(*const RequestBody, *mut ResponseBody) -> i32>
                    = lib.get(name.as_bytes()).unwrap();
                func_assoc.insert(path, func.into_raw());
            }

            (func_assoc, lib)
        };

        let mut response = ResponseBody {
            status: 0,
            body: "",
        };
        func_assoc["/hello"](&RequestBody {
            path: "/hello",
            body: "",
        }, &mut response);

        assert_eq!(response.status, 200);

    }
}