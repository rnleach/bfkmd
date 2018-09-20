//! Common code for the command line tools that manage a bufkit data archive.

//
// Public API
//
pub use cmd_line::CommonCmdLineArgs;

//
// Internal only
//
extern crate bufkit_data;
#[macro_use]
extern crate clap;
extern crate dirs;

mod cmd_line;
