use tracing::{debug, instrument};

use crate::{Frame, Connection};

#[derive(Debug)]
pub struct Unknown {
    command: String,
}

impl Unknown {
    /// 从字符中生成一个 `Unknown` 命令
    pub(crate) fn new(command: impl ToString) -> Self {
        Unknown {
            command: command.to_string(),
        }
    }

    /// 返回命令名称
    pub(crate) fn get_name(&self) -> &str {
        &self.command
    }

    /// 生成 `Unknown` 错误消息，并发送至客户端
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(&self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("Err: unknown command '{}'", self.command));

        debug!(?response);

        dst.write_frame(&response).await?;
        Ok(())
    }
}
