"""A bounded, authorized local log sink; handles never enter recorded plans."""


class OutputLog:
    def __init__(self, path, stream, limit):
        self.path, self.stream, self.limit = path, stream, limit
        self.size, self.truncated = 0, False
        self.error = None

    def write(self, data):
        if self.truncated or self.error is not None:
            return
        remaining = max(0, self.limit - self.size)
        kept = data[:remaining].decode("utf-8", errors="ignore").encode("utf-8")
        if kept and self.error is None:
            try:
                self.stream.write(kept)
                self.stream.flush()
                self.size += len(kept)
            except OSError as error:
                self.error = str(error)
        self.truncated |= len(kept) < len(data)

    def close(self):
        try:
            self.stream.close()
        except OSError as error:
            self.error = self.error or str(error)

    def summary(self):
        return {"log_path": self.path, **({"log_truncated": True} if self.truncated else {}),
            **({"log_error": self.error} if self.error else {})}
