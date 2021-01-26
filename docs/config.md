# Configuration system specification

```{contents} Table of Contents
```

## Synopsis

This proposal defines a framework involving a language description, a library, client
code examples and environment considerations for handling application configuration in
the context of Software Heritage.

## General terminology and concepts

For the purpose of this specification.

- Component: a unit comprising data and/or functions, which provides functionality
through an interface and has associated dependencies
- SWH component: a component consisting of a Python class or module, that provides a
functionality specific to one or more SWH services. The closed set of SWH components
can appear in configuration definitions.
- SWH service: collection of SWH components. Correspond roughly to docker services
developed by SWH. Examples include API servers/clients, workers, journal services, etc.

## Scope

All SWH services and components.

Use cases:
- production service: explicitly passed through the environment
- Testing: explicitly passed in code
- human CLI usage: explicitly or implicitly passed through the environment and code
- REPL: explicitly or implicitly passed through code

Configuration targets:
- WSGI: gunicorn vs Flask/aiohttp/Django devel server
- CLI entrypoint
- server application: app_from_configfile vs django config
- worker
- component: constructor/factory

Configuration sources:
- environment: CLI value, CLI path, envvar value, envvar path
- code: Python objects

## Rationale

The configuration system evolved partially with use cases. Initial design decisions
applied to all use cases turned out to not match requirements of all use cases.
Multiple ways of obtaining configuration is good for human consumption, as in
interactive CLI usage or REPL tinkering. However, implicit configuration loading hinder
debugging in production, where a good practice is to have a straightforward loading
mechanism, preferably from environment. In the testing case, explicit is also the norm,
and loading from environment makes automated testing harder. There is a need for an API
adapted to the requirements of each use case, and a standard way to application
environment handling.

Until now, configuration loading, validating and CLI definitions has had various forms.
This hetegoneneity is a maintenance burden that can be avoided by using a common
framework. In addition, most SWH components instantiate their own dependencies,
dispatching each part of the service configuration to the respective dependency. All
the instantiation, validation and composition logic of components can be taken off the
component using them, handled by the configuration system at once and at the earliest
stage of the application run: the entrypoint. This component instantiation, validation
and injection framework provide uniform handling and makes testing easier and catching
configuration bugs earlier.

Moreover, the configuration language currently lack an easy means to express both
configuration alternatives for a particular componenent and factor out common component
configuration. The need for configuration alternatives comes from the fact that, first,
for a particular service, different components may have a distinct use of a particular
component, and second, it makes easy to switch between different but common
configuration written ahead of time. By using a uniform, complete and compact way of
specifying configuration, we can get a clear overview of a service in a single unit and
ease maintenance.

(above written up from below, kept for review)

A.
- implicit/hard to follow loading: configuration may be loaded through a number of ways
automatically, useful for interactive cases but not for production cases
- dependency on environment: must be able to instantiate component using only ad-hoc
configuration, for testing purposes
-> Need for different APIs for different use cases, all compatible.

B.
- composition coupling: every owner component must know about how to instantiate an
owned component
- heterogeneity: configuration loading, validating and instantiating is implemented
differently everywhere
-> Need for dependency injection, component instantiation framework.

C.
- should be able to specify alternative configurations for one component constructed
ahead of time, and choose it at runtime/loadtime
- should be able to factor common configuration out
- uniform,complete,concise: the configuration could theoretically be centralized in one
file which would give a clear overview of the configuration and interaction between all
the components
-> Lead to definition of instances, records, references to conveniently handle those cases.

## Language description

### Target example

```yaml
storage:
  default:
    cls: buffer
    min_batch_size:
        content: 1000
    storage: <storage.filtered-uffizi>
  filtered-uffizi:
    cls: filter
    storage: <storage.uffizi>
  uffizi:
    cls: remote
    url: http://uffizi.internal.softwareheritage.org:5002/

loader-git:
  default:
    cls: remote
    storage: <storage.default>
    save_data_path: "/srv/storage/space/data/sharded_packfiles"

random-component:
  default:
    cls: foo
    celery: *celery

_:
  celery: &celery
    task_broker: amqp://...
    task_queues:
      - swh.loader.git.tasks.UpdateGitRepository
      - swh.loader.git.tasks.LoadDiskGitRepository
```

### Syntactic overview

The language is based on YAML. Specific rules are applied after YAML parsing:
- allow only YAML primitive types (includes mappings and lists), like JSON
- restrict document structure as specified below
- allow YAML aliases
- add a custom reference system where we control resolution to avoid duplicate definitions

The configuration tree is the complete and consistent tree of configuration definitions.
It is structured in 3 levels of depth: type, instance, attribute

```
type:
  instance:
    attribute
```

Component definitions are a mapping whose items are instances, with an type identifier
unique among types for a given configuration. 1 type is associated to N instances.

Instance definitions are a mapping whose items are attributes. They have an identifier
unique among instances for a given type.

Attribute is a key/value pair whose key serves as identifier. Attribute value can be any
object allowed in JSON. Thus instance definitions may contain arbitrarily nested
structure provided the base is a mapping.

References can be made to an object defined somewhere else in the tree, using a
qualified identifier.

### Identifier

An identifier is a distinguished name for either a type, an instance or an attribute. Is
a YAML string. We recommend using the `snake_case` convention, i.e. alphanumeric
characters and underscores.

In the rest of the document, identifier is abbreviated ID.

A qualified ID is a sequence of ID of the structured form `(type ID, instance ID)`. Its
string form joins each field with a dot.

```
qualified_id = type_id "." instance_id
```

### Reference

A reference is synctatically defined as a qualified identifier in string form enclosed
in chevrons. It may only appear in attribute value, potentially nested. The qualified
identifier must refer to an instance. Its source is the attribute that owns it and its
target is the object identified by the qualified ID it owns.

```
reference = "<" qualified_id ">"
```

### Type

Type of a component to be instantiated and configured. In the configuration definition,
the type correspond to the identifier at the first level. It must exist as a key in the
component type register (see below ["Library/Component register"](#component-register)).

### Instance

Instances represent alternative configurations of a given component: they have the same
type but different constructions. For example, *production* or *staging* instance.

All instances of a type must be specified in the instance level of a configuration
definition.

Instantiating one gives a Python object of the associated type, constructed from the
mapping of attributes.

For a given loaded configuration, an instance will be instantiated only once and passed
to any instances that refer to it.

### Record

Records are ad-hoc objects which store configuration that is not specific to any
component instance. They are syntactically identical to instances, with no associated
component type.

For language consistency, they live under a special dummy type ID "_".

Instantiating one gives a Python mapping structured as in the definition.

## Library

### Component register

The component type register is a `(type ID, qualified_constructor)` mapping, defined in
the configuration library.

It is used by the component resolution routine to resolve type identifiers to Python
type constructors.

Entries in this mapping are to be registered through the component registration library
routine. This registration may happen anywhere provided it is executed before using the
configuration loading API. It is advised to register the component in the package that
defines it.

`qualified_constructor` must be a Python callable returning an instance of a particular
base type. It is a callable object supporting dynamic dispatch such as a factory
function or a class.

Components that can be registered may be any SWH service component, other SWH component
or external component, which is public (= has an Python object API).

### Type implementations

This section is informational.

A component type may have multiple implementations. There is no specific support for it
in this system, but as this concept may appear in configuration, related considerations
may be worth noting.

A specific attribute of instances specifies implementation to use. It is commonly
identified as `cls`. Components that have no such feature need no such attribute in
their configuration. Alternatively, some polymorphic components may support being
instantiated without `cls`, in which case a default implementation will be used.

[rem] an indirection layer such as a factory may be defined for monomorphic components
in order to keep consistency and allow polymorphism if needed later. Alternatively, for
all components, better than a factory which is not derivable from the component type by
user code, an abstract base class constructor would abstract this indirection layer
away.

### Instantiation

Instantiating is the process through which a concrete object is constructed from a
model and parameters. In the context of this system, a Python object is created though
calling its constructor with the set of attributes associated to a particular instance
in a configuration definition.

The input is a qualified ID identifying an instance and a configuration tree containing the
instance and its dependencies (reference targets). The output is a component instance
of the base type associated with the type ID contained in the qualified ID. The process
is composed of the following steps in order.

1. Fetch the instance mapping by qualified ID in the configuration.
2. Resolve references to instance definitions.
3. Recurse on referenced instances to instantiate each.
4. Compose instances, i.e. replace references by the corresponding instatiated
definition.
5. Resolve the type ID contained in the qualified ID of the instance, to a component
constructor.
6. Call the component constructor, passing the updated instance mapping as arguments.

An instance identified as "default" is instantiated if a type ID but no instance ID is
provided to the instantiation routine.

Instances must be instantiated only once and used at each reference source.

Records are instantiated as a tree whose root is a mapping, and subsequent levels may
be any JSON-like object.

### Interpretation

This section is informational.

Interpretation of attributes beyond stated above is out of scope and left to the
component constructors to do.

Standard Python typing available in constructors may be used to as the basis for the
validation of configuration data. Validity of structure, value and existence may be
checked. Conversions may also be performed.

To ease the validation process which can be repetitive and cubersome, the library
provides generic validation primitives and a validation routine based on a data model
specification object, described at ["Library/Validation API"](#validation-api).

### Loading

Loading is the process of fetching data from a data source into a memory space which is
more easily accessible to the processing system. In the context of this system, this
data is then read and converted into a Python object.

Loading source may be: an I/O file abstraction (whatever its backing source), or an
operating system path to such file abstraction, or such path resolvable from an
environment variable.

Only a Python dictionary is accepted as the holder of this data once loaded. A default
configuration definition, either as a dictionary literal or a loaded configuration, can
be specified in which case every attributes absent from loaded configuration will be
set to the default one.

### API overview

Library should be imported as `config` everywhere for clarity and uniformity (e.g.
`import swh.core.config as config` or `from swh.core import config`).

[rem]: Existing routine `merge_configs` should be moved to another module as `merge_dicts`.

WARNING:

In the following examples, names subject to change.
Code is inspired by Python, but abstracted to focus on datatypes.
`DeriveType` denotes simply a type derived from an exiting one, with no consideration
of compatibility with base type or any other.

### Loading API

[rem]: should choose term among `load`, `read`, `from`, `by`, `config`
Example names: `read_config`, `load_envvar`

```python
Config = DeriveType(Mapping) # whole tree, whose first three levels are mappings
Envvar = DeriveType(str)
File = io.IOBase
Path = os.PathLike

load: (Union[File,Path,Envvar]) -> (Config)
load_from_file: (File) -> (Config)
load_from_path: (Path) -> (Config)
load_from_envvar: (Envvar) -> (Config)
```

Loads as YAML tree and convert to Python recursive mapping.

[Q]:
- Where to check for loadable path? Library routines or user code? May duplicate behavior.
- Should envvar be hardcoded in library or default? Same for default path.

### Instantiation API

[rem] Should choose among `get`, `read`, `from_config`, `instantiate`, `component`, `instance`.
Example names: `get_component_from_config`, `instantiate_from_config`, `create_component`,
`read_instance`, `get_from_id`

```python
QualifiedID = (TypeID, InstanceID)
Component = DeriveType(type)
InstanceConfig = DeriveType(Mapping)
```

```python
create_component: (Config, TypeID, InstanceID) -> (Component)
```

Returns an instantiated component identified by qualified ID. The type ID must exist in
the register. Cannot be used to get a record.

```python
get_instance: (Config, TypeID, InstanceID) -> (InstanceConfig)
```

Returns the instance definition (instance or record) identified by the qualified ID,
unprocessed. The output is a tree whose root is a mapping. Use it to get a record,
or get the definition of an instance.

### Validation API

This section proposes a framework for validating instance definitions in a fairly
lightweight and flexible way, for use by component constructors or injectors.

```python
check: (Config) -> (Boolean)
check_definitions: (Config) -> (Boolean)
check_component: (InstanceConfig, ModelSpec) -> (Boolean)
generate_spec_from_signature: (ComponentConstructor) -> (ModelSpec)
```

`check`: validate both language and instances.
`check_definitions`: validate whole definition against language spec.
`check_component`: validate component instance definition against component spec.
This is a template function which is parametrized by user-specified spec.

[Q]: should those functions return a boolean or raise exception?

#### Model specification

```python
AttrKey ~= String("[A-Za-z0-9_\-]+")
AttrVal = YAML_object
# Path in the instance configuration mapping
Path ~= String("([A-Za-z0-9_\-]+/)+")

# Wrapper to convert falsey values or exceptions to False, otherwise True
ensure_boolean: Booleanish -> Boolean
# Generic and context-sensitive signatures for flexibility
value_check: ((AttrVal) | (AttrVal, InstanceConfig)) -> Booleanish
# If not optional existence check should succeed, else not performed.
optional_check: (AttrVal, InstanceConfig) -> Booleanish) | Booleanish
# Checks whether attr exists at one of given paths, or anywhere if no path.
# No reason to have user customise existence check.
existence_check: (AttrVal, Set(Path), InstanceConfig) -> Boolean

# Here is the model specification
# Kwargs: best I found for a typed mapping where every item is optional
AttrProperties = Kwargs(value_check, optional_check, Set(Path))
# None for no checks on attribute
ModelSpec = Mapping(AttrKey, AttrProperties | None)
```

`check_component` verifies that all properties of every attribute holds in the instance
definition, based on user-defined model specification. Model specification can leverage
primitive check functions and user-defined check functions. Supported checks are value
and existence in tree-structure checks, which are distinguished for expressiveness.

The model specification lists each (unqualified) attribute that may exist in the
configuration definition, along with attribute properties that must hold.

An attribute may or not be optional, meaning whether validation should fail on absence,
based on the boolean value of the `optional_check`. `optional_check` may be a callable
that must determine whether the attribute is optional based on the configuration
context and return a booleanish value, or be a booleanish value. It is run in a wrapper
which converts falsey values or exceptions to `False`, and anything else to `True`.
Required attribute is checked for existence based on a set of paths in the tree if any,
or existence anywhere in the tree. Optional attribute is then not checked for existence
but still for legal value.

The value check may be any callable that either accepts a single value, or a value and
the configuration context (instance definition), and return a booleanish value, handled
as above. This makes it possible to use many existing functions or object constructors
to do the validation, e.g. `int`, `re.match`, `isinstance(Protocol)` or a function
verifying a relation to another attribute in the definition is valid.

#### Helper for specification generation

`generate_spec_from_signature`: generate a model specification where annotations are
used as `value_check` functions wherever possible, argument are optional or not
depending on the existence a default value, and the path set contains only the tree
root. A mapping from types to validators is used to validate most common types, others
will only be checked by `insinstance`. This is a helper function to generate a spec
draft ahead of time, that must be corrected and stored along the corresponding
constructor, as it is generic and one may want a different set of validators.

[rem] components with multiple implementations:

Operations based on function signatures like validation but also instantiation, need a
way to map the `cls` argument to the concrete type and constructor signature. A
solution to automatically use the good constructor is to implement single dispatch and
overloading on the main constructor. Every method may still call the main one, but must
have a signature compatible with the one of the concrete class constructor, based on
`cls`.

See also ["Library/Type implementations"](#type-implementations) remark about abstract
constructors.

## Client code

Demonstration of features in every use cases.

CLI, WSGI, worker, task, daemon, testing

### CLI entrypoint

Example: scan the current directory against the archive in docker

CLI option usage: `swh scanner --scanner-instance=docker scan .`
Environment variable usage: `SWH_SCANNER_INSTANCE=docker swh scanner scan .`

Python snippet in the CLI endpoint:

```python
import swh.core.config as config
scanner_instance = "docker"  # from cli option or envvar
config_path = "~/.config/swh/default.yml"  # from cli option or envvar or library default
config_dict = config.load(config_path)
scanner = config.create_component(
    config_dict,
    type="scanner",
    instance=scanner_instance
)  # destructured QualifiedID definition, using type and instance keys

scanner.scan(".")
```

### API Server entrypoint

```python
def make_app_from_configfile() -> StorageServerApp:
    global app_instance
    if not app_instance:
        config_dict = config.load_from_envvar()
        rpc_instance = os.environ.get("SWH_STORAGE_RPC_INSTANCE", "default")
        app_instance = config.create_component(
            config_dict,
            type="storage-rpc",
            instance=rpc_instance
        )
        check_component(app_instance, "storage-rpc")  # should raise or return boolean?
    return app_instance
```

### Celery task entrypoint

```python
@shared_task(name="foo.bar")
def load_git(url):
    config_dict = config.load_from_envvar()
    loader_instance = os.environ.get("SWH_LOADER_GIT_INSTANCE", "default")
    loader = config.create_component(
        config_dict,
        type="loader-git",
        instance=loader_instance
    )
    config_dict.create_component(type="loader-git", instance=loader_instance)
    return loader.load()
```

### Testing / REPL

Example test

```python
import swh.core.config as config

@pytest.fixture
def config_dict() {
    return {...}
}


def test_config(config_dict):
    type_id = "objstorage"
    instance_id = "test_1"
    instance = config.create_component(
        config_dict,
        config.QualifiedID(type=type_id, instance=instance_id)
    )  # canonical QualifiedID definition
    ...


@pytest.fixture
def config_path(datadir):
    return f"{datadir}/other.yml"


def test_config2(config_path):
    config_dict = config.load_from_path(config_path)
    instance = config.create_component(
        config_dict,
        config.QualifiedID(type=type_id, instance=instance_id)
    )
    ...
`

## Environment

The environment parameters comprises any dependency of the configuration system
external to the code. This includes: configuration directory, configuration file,
environment variable and commandline parameters.

### Configuration directory

SWH default configuration directory, used when no configuration path is provided:
`SWH_CONFIG_HOME=$HOME/.config/swh`

### Configuration file

YAML file with `.yml` extension containing only the configuration data.
Default if none is specified to the generic loading routine: `$SWH_CONFIG_HOME/default.yml`.

### Core configuration file parameter

This feature is to be built into SWH core library.
Specify the path to the configuration file to use for a whole service:
path_part = `path` | `file`
Environment variable: `SWH_CONFIG_<PATH_PART>`
CLI option: `swh --config-<path_part>`

[rem]: "path" is a more precise term than "file".

### Specific configuration parameters

A CLI option may be passed to specify an instance ID (only at 2nd level) when several
alternatives are provided in the configuration. Such option must be declared statically
in CLI code.

Specify the instance configuration to use for a given component, using an instance ID:
id_part = `instance` | `id` | `iid` | `cid`
`SWH_<COMP>_<ID_PART>` `--<comp>-<id_part>`

[rem]: any variant containing "id" is more precise than simply "instance".

A CLI option may be passed to override an attribute in the configuration.
Such option must be declared statically in CLI code.

Specify any other predefined configuration option:
`SWH_<COMP>_<OPTION>` `--<comp>-<option>`

### Configuration priority (need contributions)

CLI has precedence over envvars.
Environment parameters have precedence over whole definitions (from file or code) and
whole definitions have precedence over defaults, per-attribute.
This follows the principle that the particular takes precedence over the general.

CLI param > envvar param > CLI file > envvar file > defaults file > defaults literal

This precedence rules must be implemented in entrypoint client code, with the help of
the library loading API. Only part of it may be implemented, the minimum being
accepting a whole definition trough either code or envvar.

### Example environment specifications (need contributions)

Using this objstorage replayer configuration file.

```yaml
objstorage:
  local:
    cls: pathslicing
    root: /srv/softwareheritage/objects
    slicing: "0:2/2:5"
  s3:
    cls: s3
    s3-param: foo

journal-client:
  default:
    # single impl: no cls needed
    brokers:
      - kafka
    prefix: swh.journal.objects
    client-param: bar
  docker:
    brokers:
      - kafka.swh-dev.docker

objstorage-replayer:
  default:
    src: <objstorage.local>
    dst: <objstorage.s3>
    journal-client: <journal-client.default>
```

CLI usage:

Specify no instance, use default instance config:
- `swh objstorage replayer`

Specify instance:
- `swh objstorage replayer --instance default`

Specify nested instances:

- `swh objstorage replayer --src local --dst s3`
- `swh objstorage replayer --src local --dst s3 --journal-client docker`

CLI options must be defined statically (as opposed to be handled dynamically following
a regular scheme) in CLI endpoints.

#### Other proposals

@douardda's proposal: on-the-fly generation of instance config via syntactic sugar
`swh objstorage replayer --src
"pathslicing://?root=/srv/softwareheritage/objects&slicing=0:2/2:5" --dst s3`

That is implementing specific URI schemes for components to compactly define their
configuration.


## Limitations (need contributions)

Currently does not provide a canonical way to design and implement configuration
handling, but provide a flexible framework and some recommendations, as the discussion
was oriented more around better tooling than specific rules.
Maybe TODO add a section about that?


## Out of scope and rejected ideas

Defining anonymous instances directly in instance definitions, adding a specification
of its type. This introduces moderate complexity in library implementation and make it
less regular, but simplifies some definitions.
Out of scope.

Reference not only instances, but also attributes (in records or not) through our
reference system, specifying them with Attribute IDs. We chose to use YAML references
to reference attributes.
Alternative solution chosen.

Validation API. Specified, but Out of scope for now.

Configuration file identifiers without base path and extension, leaving to the library
to resolve it to a path in known locations.
Rejected.

Merging support in loading functions with default configuration.
Rejected?

References between partial or whole configuration definitions. Would add too much complexity.
Rejected.

@tenma's proposal on env params: dynamic handling wrt schema, similar to what `click` permits.
`swh objstorage replayer --objstorage-replayer.src=objstorage.local --objstorage-replayer.journal-client=journal-client.default`
This too generic and verbose for our uses, but permits arbitrary attribute setting.
Rejected?

## Impacts (need contributions)

- core.config library
- main constructor/factory of each SWH component: type mapping or dynamic dispatch, use
of library APIs for validating
- entrypoints: use of library APIs for loading and instantiating
- configuration files format
- environment variables and cli calls in production+docker environments

## Implementation plan: library, ops code, tests, prod (need contributions)

(Proposition)

Prepare for easy switch and rollback by creating configuration copies conforming to the
new system, and code conforming to the new system in separate branches.

- implement all library in the same file as before
- migrate tests (at any moment)
- prepare new config files, and service definitions that use them
- migrate services one by one following SWH dependencies:
    - add needed declarations along with constructors
    - entrypoint loading, instantiating and injecting (if opt=subinst)
- remove deprecated code
