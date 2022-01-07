pub struct ErrLogger {

}

impl ErrLogger {
    pub fn new() -> ErrLogger {
        ErrLogger {}
    }

    pub fn log(&self, msg: &str) {
        println!("Warn: {:?}", msg);
    }
}

impl Clone for ErrLogger {
    fn clone(&self) -> ErrLogger {
        ErrLogger {}
    }
}