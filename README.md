# qp = db⁻¹: inverting db complexity 🧠

`qp` is a robust, yet lightweight, library designed to supercharge your database layer, seamlessly fitting into your application's architecture.

## Table of Contents

- [Quick Start](#user-content-quick-start)
- [API Reference](#user-content-api-reference)
    - [Insert Operations](#user-content-insert-operations)
    - [Select Operations](#user-content-select-operations)
    - [Update Operations](#user-content-update-operations)
    - [Delete Operations](#user-content-delete-operations)

## Quick Start

`qp` stands on the shoulders of the [sqlx](https://github.com/jmoiron/sqlx) library, leveraging its capabilities to map data model structs to named query parameters. It serves as an augmentation layer, providing an opinionated but flexible CRUD API.

To get started, define your model structs with an embedded `qp.Base`:

```go
package db

import (
    "jig.sx/qp"
    "jig.sx/qp/qpjson"
)

type User struct {
    qp.Base

    Username string      `db:"username"`
    Address  string      `db:"address"`
    Attrs    qpjson.Type `db:"attrs"`
}
```

Now, access a range of CRUD functionalities via `qp.Table[model.User]`:

```go
package db

import (
    "fmt"

    "jig.sx/qp"
    "jig.sx/qp/qpjson"
)

type DB struct {
    Users *qp.Table[User]
}

func New(dsn string) (*DB, error) {
    db, err := qp.Open(dsn)
    if err != nil {
        return nil, fmt.Errorf("Failed to connect to db: %w", err)
    }

    return &DB{
        Users: qp.NewTable[User]("users", "usr", db),
    }
}
```

## API Reference

Utilize `qp.Table` for CRUD operations, all aligned with best practices.

### Insert Operations

```go
func (t *Table[U]) Insert(ctx context.Context, u *U) error
```

**Usage:**

```go
user := &db.User{
    Username: "johndoe",
    Address:  "http://github.com/johndoe",
    Attrs:    qpjson.Object("admin", true),
}

err := db.Users.Insert(ctx, user)
```

This populates:

```go
user.CreatedAt // time.Time
user.UpdatedAt // time.Time
user.ID        // "key-4r2m96V48qRjWbXM"
```

### Select Operations

```go
func (t *Table[U]) Select(ctx context.Context, args ...any) ([]*U, error)
```

**Usage:**

```go
users, err := db.Users.Select(ctx, "username", "johndoe")
// or
admins, err := db.Users.Select(ctx, qpjson.Path{"attrs", "admin"}, true)
```

### Update Operations

```go
func (t *Table[U]) Update(ctx context.Context, id string, args ...any) error
```

**Usage:**

```go
err := db.Users.Update(ctx, user.ID, qpjson.Path{"attrs", "admin"}, false)
```

### Delete Operations

```go
func (t *Table[U]) Delete(ctx context.Context, id string) error
```

**Usage:**

```go
err := db.Users.Delete(ctx, user.ID)
```

This performs a soft delete:

```go
user.DeletedAt // time.Time
```

That's `qp`—CRUD operations made quick, clean, and extendable.
