import { mkdir, open, rename, unlink, lstat, rmdir } from "node:fs/promises";
import { constants } from "node:fs";
import { join } from "node:path";
import { randomUUID } from "node:crypto";

/** Single-writer durable bridge state. Contains conversation data, never credentials. */
export class StateStore {
  constructor(directory, maxBytes = 32 * 1024 * 1024) {
    this.directory = directory;
    this.path = join(directory, "state.json");
    this.lock = join(directory, ".writer-lock");
    this.maxBytes = maxBytes;
    this.state = { version: 1, bindings: {} };
    this.writes = Promise.resolve();
  }

  async load() {
    await mkdir(this.directory, { recursive: true, mode: 0o700 });
    const directory = await lstat(this.directory);
    if (
      !directory.isDirectory() ||
      directory.isSymbolicLink() ||
      directory.mode & 0o077
    ) {
      throw new Error(
        "Bridge state directory must be a private directory (mode 0700)",
      );
    }
    await mkdir(this.lock, { mode: 0o700 }); // Fail closed on a second writer, including uncertain crash recovery.
    this.locked = true;
    try {
      const file = await open(
        this.path,
        constants.O_RDONLY | constants.O_NOFOLLOW,
      );
      try {
        const stat = await file.stat();
        if (!stat.isFile() || stat.mode & 0o077 || stat.size > this.maxBytes)
          throw new Error("Unsafe bridge state file");
        const value = JSON.parse(await file.readFile("utf8"));
        if (
          value.version !== 1 ||
          !value.bindings ||
          Array.isArray(value.bindings) ||
          typeof value.bindings !== "object"
        ) {
          throw new Error("Unsupported bridge state format");
        }
        this.state = value;
      } finally {
        await file.close();
      }
    } catch (error) {
      if (error.code !== "ENOENT") {
        await this.close();
        throw error;
      }
    }
    return this;
  }

  save() {
    const bytes = JSON.stringify(this.state);
    if (Buffer.byteLength(bytes) > this.maxBytes)
      return Promise.reject(new Error("Bridge state exceeds configured limit"));
    const write = this.writes.then(async () => {
      if (!this.locked) throw new Error("Bridge state is not locked");
      const temporary = join(this.directory, `.state-${randomUUID()}.tmp`);
      const file = await open(
        temporary,
        constants.O_WRONLY |
          constants.O_CREAT |
          constants.O_EXCL |
          constants.O_NOFOLLOW,
        0o600,
      );
      try {
        await file.writeFile(bytes);
        await file.sync();
      } finally {
        await file.close();
      }
      try {
        await rename(temporary, this.path);
        const directory = await open(this.directory, constants.O_RDONLY);
        try {
          await directory.sync();
        } finally {
          await directory.close();
        }
      } catch (error) {
        await unlink(temporary).catch(() => {});
        throw error;
      }
    });
    this.writes = write.catch(() => {});
    return write;
  }

  async close() {
    await this.writes;
    if (this.locked) {
      this.locked = false;
      await rmdir(this.lock);
    }
  }
}
