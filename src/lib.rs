//! Common code for the command line tools that manage a bufkit data archive.

//
// Public API
//
pub use util::bail;

//
// Internal only
//
mod util {
    pub fn bail(msg: &str) -> ! {
        println!("{}", msg);
        ::std::process::exit(1);
    }
}
