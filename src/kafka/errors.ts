/** Base class for dependency availability issues */
export class DependencyUnavailableError extends Error {
  constructor(message: string, public readonly dependency: string) {
    super(message);
    this.name = "DependencyUnavailableError";
  }
}

export class DBUnavailableError extends DependencyUnavailableError {
  constructor(message = "Database unavailable") {
    super(message, "database");
    this.name = "DBUnavailableError";
  }
}

export class APIUnavailableError extends DependencyUnavailableError {
  constructor(message = "Downstream API unavailable") {
    super(message, "api");
    this.name = "APIUnavailableError";
  }
}

// Add more specific errors if needed; your predicate can check instanceof.
