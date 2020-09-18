[![GoDoc](https://godoc.org/github.com/jamiealquiza/envy?status.svg)](https://godoc.org/github.com/jamiealquiza/envy)


# envy

Automatically exposes environment variables for all of your flags. It supports the standard flags package along with limited support for [Cobra](https://github.com/spf13/cobra) commands.

Envy takes a namespace prefix that will be used for environment variable lookups. Each flag registered in your app will be prefixed, uppercased, and hyphens exchanged for underscores; if a matching environment variable is found, it will set the respective flag value as long as the value is not otherwise explicitly set (see usage for precedence).

### Example: flag

Code:
```go
package main

import (
        "flag"
        "fmt"

        "github.com/jamiealquiza/envy"
)

func main() {
        var address = flag.String("address", "127.0.0.1", "Some random address")
        var port = flag.String("port", "8131", "Some random port")

        envy.Parse("MYAPP") // Expose environment variables.
        flag.Parse()

        fmt.Println(*address)
        fmt.Println(*port)
}
```

Output:
```sh
# Prints flag defaults
% ./example
127.0.0.1
8131

# Setting flags via env vars.
% MYAPP_ADDRESS="0.0.0.0" MYAPP_PORT="9080" ./example
0.0.0.0
9080
```

### Example: Cobra

Code:
```go

// Where to execute envy depends on the structure
// of your Cobra implementation. A common pattern
// is to define a root command and an 'Execute' function
// that's called from the application main. We can call
// envy ParseCobra here and configure it to recursively
// update all child commands. Alternatively, it can be
// scoped to child commands at some point in their
// initialization.

var rootCmd = &cobra.Command{
 Use: "myapp",
}

func Execute() {
  // Configure envy.
  cfg := CobraConfig{
    // The env var prefix.
    Prefix: "MYAPP",
    // Whether to parse persistent flags.
    Persistent: true,
    // Whether to recursively update child command FlagSets.
    Recursive: true,
  }

  // Apply.
  envy.ParseCobra(rootCmd, cfg)

  if err := rootCmd.Execute(); err != nil {
    fmt.Println(err)
    os.Exit(1)
  }
}
```

Output:
```sh
# Root command flags.
% myapp
Usage:
  myapp [command]

Available Commands:
  help        Help about any command
  doathing    This is a subcommand

Flags:
  -h, --help               help for myapp
      --some-config        A global config [MYAPP_SOME_CONFIG]

Use "myapp [command] --help" for more information about a command.

# Child command flags. Notice that the prefix
# has the subcommand name automatically appended
# while preserving global/parent env vars.
% myapp doathing
Usage:
  myapp doathing [flags]

Flags:
  -h, --help               help for myapp
      --subcmd-config      Another config [MYAPP_DOATHING_SUBCMD_CONFIG]

Global Flags:
      --some-flag          A global flag [MYAPP_SOME_FLAG]
```

### Usage

**Variable precedence:**

Envy results in the following order of precedence, each item overwriting the previous:
`flag default` -> `Envy generated env var` -> `flag set at the CLI`.

Results referencing the stdlib flag example code:
- `./example` will result in `port` being set to `8131`
- `MYAPP_PORT=5678 ./example` will result in `port` being set to `5678`
- `MYAPP_PORT=5678 ./example -port=1234` will result in `port` being set to `1234`


**Env vars in help output:**

Envy can update your app help output so that it includes the environment variable generated/referenced for each flag. This is done by calling `envy.Parse()` before `flag.Parse()`.

The above example:
```
Usage of ./example:
  -address string
        Some random address [MYAPP_ADDRESS] (default "127.0.0.1")
  -port string
        Some random port [MYAPP_PORT] (default "8131")
```

 If this isn't desired, simply call `envy.Parse()` after `flag.Parse()`:
```go
// ...
	flag.Parse()
        envy.Parse("MYAPP") // looks for MYAPP_ADDRESS & MYAPP_PORT
// ...
```

```
Usage of ./example:
  -address string
        Some random address (default "127.0.0.1")
  -port string
        Some random port (default "8131")
```

**Satisfying types:**

Environment variables should be defined using a type that satisfies the respective type in your Go application's flag. For example:
- `string` -> `APP_ASTRINGVAR="someString"`
- `int` -> `APP_ANINTVAR=42`
- `bool` -> `APP_ABOOLVAR=true`

**Side effects:**

Setting a flag through an Envy generated environment variable will have the same effects on the default `flag.CommandLine` as if the flag were set via the command line. This only affect users that may rely on `flag.CommandLine` methods that make distinctions between set and to-be set flags (such as the `Visit` method).

**Cobra compatibility:**

The extensive types in Cobra's underlying [pflag](https://github.com/spf13/pflag) have not been tested, hence the "limited support" reference.

Also, keep in mind that Cobra can change in a way that breaks support with envy. Functionality was tested as of 2018-11-19.
