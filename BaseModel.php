<?php

declare(strict_types=1);

namespace SimpleDb;

use Exception;
use Generator;
use PDO;
use PDOException;
use PDOStatement;

/**
 * --------------------------------------------------
 * BaseModel Class
 * --------------------------------------------------
 *
 * A lightweight PDO wrapper for MVC applications.
 *
 * @category Database Access
 * @package  SimpleDb
 * @license  LGPL v3 http://opensource.org/licenses/lgpl-3.0.html
 */
class BaseModel
{
    /**
     * Database credentials
     */
    private array $connectionParams = [
        'type'     => 'mysql', // ('sqlite', 'mysql', 'pgsql', 'sqlsrv')
        'host'     => null,
        'username' => null,
        'password' => null,
        'dbname'   => null,
        'port'     => null,
        'charset'  => 'utf8mb4', // Default modernization
        'prefix'   => ''
    ];

    /** @var bool FOR UPDATE flag */
    private bool $forUpdate = false;

    /** @var array Dynamic type list for group by condition value */
    private array $groupBy = [];

    /** @var array An array that holds having conditions */
    private array $having = [];

    /** @var BaseModel|null Static instance of self */
    private static ?BaseModel $instance = null;

    /** @var bool Is Subquery object */
    private bool $isSubQuery = false;

    /** @var array An array that holds where joins */
    private array $join = [];

    /** @var array|null Last error information */
    private ?array $lastError = [];

    /** @var string|null Last error code */
    private ?string $lastErrorCode = '';

    /** @var string|int|null Name of the auto increment column */
    private string|int|null $lastInsertId = null;

    /** @var string The previously executed SQL query */
    private string $lastQuery = '';

    /** @var bool LOCK IN SHARE MODE flag */
    private bool $lockInShareMode = false;

    /** @var bool Should join() results be nested by table */
    private bool $nestJoin = false;

    /** @var array Dynamic type list for order by condition value */
    private array $orderBy = [];

    /** @var bool include Limit clause in the QueryBuild statement */
    private bool $isRecordCountQuery = false;

    /** @var int Rows per 1 page on paginate() method */
    private int $pageLimit = 10;

    /** @var array Binded params */
    private array $params = [];

    /** @var PDO|null PDO instance */
    private ?PDO $pdo = null;

    /** @var string Database prefix */
    private string $prefix = '';

    /** @var string Query string */
    private string $query = '';

    /** @var string Quote for field name */
    private string $fieldQuote = '`';

    /** @var array The SQL query options */
    private array $queryOptions = [];

    /** @var string Query type */
    private string $queryType = '';

    /** @var int Type of returned result */
    private int $returnType = PDO::FETCH_ASSOC;

    /** @var int Number of affected rows */
    private int $rowCount = 0;

    /** @var bool Transaction flag */
    public bool $transaction = false;

    /** @var int Total rows for withTotalCount() */
    public int $totalCount = 0;

    /** @var int Total pages of paginate() method */
    public int $totalPages = 0;

    /** @var array|null Column names for update when using onDuplicate method */
    protected ?array $updateColumns = null;

    /** @var bool Option to use generator (yield) */
    private bool $useGenerator = false;

    /** @var array An array that holds where conditions */
    private array $where = [];

    /**
     * @param string|array|object $type Or connection params array/PDO object
     * @param string|null $host
     * @param string|null $username
     * @param string|null $password
     * @param string|null $dbname
     * @param int|null $port
     * @param string|null $charset
     */
    public function __construct(
        string|array|object $type,
        ?string $host = null,
        ?string $username = null,
        ?string $password = null,
        ?string $dbname = null,
        ?int $port = null,
        ?string $charset = null
    ) {
        if (is_array($type)) {
            // Merge defaults with provided array, filtering out nulls
            $this->connectionParams = array_merge($this->connectionParams, array_filter($type, fn($value) => !is_null($value)));
        } elseif ($type instanceof PDO) {
            $this->pdo = $type;
        } else {
            // Explicit assignment instead of variable variables ($$key)
            if ($type) $this->connectionParams['type'] = (string)$type;
            if ($host) $this->connectionParams['host'] = $host;
            if ($username) $this->connectionParams['username'] = $username;
            if ($password) $this->connectionParams['password'] = $password;
            if ($dbname) $this->connectionParams['dbname'] = $dbname;
            if ($port) $this->connectionParams['port'] = $port;
            if ($charset) $this->connectionParams['charset'] = $charset;
        }

        if (isset($this->connectionParams['prefix'])) {
            $this->setPrefix($this->connectionParams['prefix']);
        }

        if (isset($this->connectionParams['isSubQuery']) && $this->connectionParams['isSubQuery'] === true) {
            $this->isSubQuery = true;
            return;
        }

        // Set quotes based on DB Type
        if (!empty($this->connectionParams['type'])) {
            $dbType = strtolower($this->connectionParams['type']);
            $this->fieldQuote = ($dbType === 'mysql') ? '`' : '"';
        }

        // Set static instance if not already set (Singleton-ish behavior)
        if (!self::$instance) {
            self::$instance = $this;
        }
    }

    /**
     * Connect to the database.
     *
     * @throws Exception
     */
    public function connect(): void
    {
        if ($this->pdo) {
            return;
        }

        if (empty($this->connectionParams['type'])) {
            throw new Exception('DB Type is not set.');
        }

        $type = strtolower($this->connectionParams['type']);

        try {
            if ($type === 'sqlite') {
                $this->fieldQuote = '"';
                $connectionString = 'sqlite:' . $this->connectionParams['dbname'];
                $this->pdo = new PDO($connectionString);
            } elseif ($type === 'sqlsrv') {
                $this->fieldQuote = '"';
                $dsn = $this->connectionParams['host'];
                $dbname = $this->connectionParams['dbname'];
                // SQLSrv uses different connection string format
                $connectionString = "sqlsrv:server=$dsn;database=$dbname";
                $this->pdo = new PDO($connectionString, $this->connectionParams['username'], $this->connectionParams['password']);
            } else {
                // MySQL, PgSQL
                $connectionString = $this->connectionParams['type'] . ':';
                $params = ['host', 'dbname', 'port'];

                foreach ($params as $param) {
                    if (!empty($this->connectionParams[$param])) {
                        $connectionString .= $param . '=' . $this->connectionParams[$param] . ';';
                    }
                }

                $connectionString = rtrim($connectionString, ';');
                $connectionString .= ';charset=' . ($this->connectionParams['charset'] ?? 'utf8mb4');

                $options = [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                    PDO::ATTR_EMULATE_PREPARES => false,
                ];

                $this->pdo = new PDO(
                    $connectionString,
                    $this->connectionParams['username'],
                    $this->connectionParams['password'],
                    $options
                );
            }
        } catch (PDOException $e) {
            throw new Exception('Database Connection Error: ' . $e->getMessage());
        }
    }

    /**
     * Internal method to build WHERE conditions
     */
    private function buildCondition(string $operator, array $conditions): void
    {
        if (empty($conditions)) {
            return;
        }

        $this->query .= ' ' . $operator;

        foreach ($conditions as $cond) {
            list($concat, $varName, $op, $val) = $cond;
            $this->query .= " " . $concat . " " . $varName;

            $opLower = strtolower($op);

            switch ($opLower) {
                case 'not in':
                case 'in':
                    $comparison = ' ' . $op . ' (';
                    if ($val instanceof BaseModel) {
                        $comparison .= $this->buildPair("", $val);
                    } elseif (is_array($val)) {
                        foreach ($val as $v) {
                            $comparison .= ' ?,';
                            $this->params[] = $v;
                        }
                    }
                    $this->query .= rtrim($comparison, ',') . ' ) ';
                    break;
                case 'not between':
                case 'between':
                    $this->query .= " $op ? AND ? ";
                    if (is_array($val)) {
                        $this->params = array_merge($this->params, $val);
                    }
                    break;
                case 'not exists':
                case 'exists':
                    $this->query .= $op . $this->buildPair("", $val);
                    break;
                default:
                    if (is_array($val)) {
                        $this->params = array_merge($this->params, $val);
                    } elseif ($val === null || $val === 'DBNULL') {
                        $this->query .= ' ' . $op . " NULL";
                    } else {
                        $this->query .= $this->buildPair($op, $val);
                    }
            }
        }
    }

    /**
     * Build Insert/Update pairs
     */
    private function buildDataPairs(array $tableData, array $tableColumns, bool $isInsert): void
    {
        foreach ($tableColumns as $column) {
            $value = $tableData[$column];
            $quote = $this->fieldQuote;

            if (!$isInsert) {
                if (strpos($column, '.') === false) {
                    $this->query .= "$quote" . $column . "$quote = ";
                } else {
                    $this->query .= str_replace('.', ".$quote", $column) . "$quote = ";
                }
            }

            // Subquery value
            if ($value instanceof BaseModel) {
                $this->query .= $this->buildPair("", $value) . ", ";
                continue;
            }

            // Simple value
            if (!is_array($value)) {
                $this->query .= '?, ';
                $this->params[] = $value;
                continue;
            }

            // Function value array key handling
            $key = key($value);
            $val = $value[$key];

            switch ($key) {
                case '[I]':
                    $this->query .= $column . $val . ", ";
                    break;
                case '[F]':
                    $this->query .= $val[0] . ", ";
                    if (!empty($val[1]) && is_array($val[1])) {
                        foreach ($val[1] as $param) {
                            $this->params[] = $param;
                        }
                    }
                    break;
                case '[N]':
                    if ($val == null) {
                        $this->query .= "!" . $column . ", ";
                    } else {
                        $this->query .= "!" . $val . ", ";
                    }
                    break;
                default:
                    // Fallback for weird array structures, safer to error out or encode
                    throw new Exception("Invalid data structure for column $column");
            }
        }
        $this->query = rtrim($this->query, ', ');
    }

    protected function buildGroupBy(): void
    {
        if (empty($this->groupBy)) {
            return;
        }

        $this->query .= " GROUP BY " . implode(", ", $this->groupBy) . " ";
    }

    private function buildInsert(string $tableName, array $insertData, string $operation): int|bool
    {
        $this->query         = $operation . implode(' ', $this->queryOptions) . ' INTO ' . $this->getTableName($tableName);
        $this->queryType     = $operation;
        $stmt                = $this->buildQuery(null, $insertData);

        if (!$stmt) return false;

        try {
            $status              = $stmt->execute();
            $this->rowCount      = $stmt->rowCount();
            $this->lastError     = $stmt->errorInfo();
            $this->lastErrorCode = $stmt->errorCode();
            $this->reset();

            if ($status && $this->pdo()->lastInsertId() > 0) {
                return (int) $this->pdo()->lastInsertId();
            }

            return $status;
        } catch (PDOException $e) {
            $this->lastError = [$e->getCode(), $e->getCode(), $e->getMessage()];
            return false;
        }
    }

    private function buildInsertQuery(?array $tableData): void
    {
        if (!is_array($tableData)) {
            return;
        }
        $isInsert    = in_array($this->queryType, ['REPLACE', 'INSERT']);
        $dataColumns = array_keys($tableData);
        $quote = $this->fieldQuote;

        if ($isInsert) {
            if (isset($dataColumns[0])) {
                $this->query .= " ($quote" . implode("$quote, $quote", $dataColumns) . "$quote) ";
            }
            $this->query .= ' VALUES (';
        } else {
            $this->query .= " SET ";
        }

        $this->buildDataPairs($tableData, $dataColumns, $isInsert);

        if ($isInsert) {
            $this->query .= ')';
        }
    }

    private function buildJoin(): void
    {
        if (empty($this->join)) {
            return;
        }

        foreach ($this->join as $data) {
            list($joinType, $joinTable, $joinCondition) = $data;

            if ($joinTable instanceof BaseModel) {
                $joinStr = $this->buildPair("", $joinTable);
            } else {
                $joinStr = $joinTable;
            }

            $this->query .= " " . $joinType . " JOIN " . $joinStr .
                (stripos($joinCondition, 'using') !== false ? " " : " ON ")
                . $joinCondition;
        }
    }

    private function buildLimit(int|array|null $numRows): void
    {
        if ($numRows === null) {
            return;
        }

        if (is_array($numRows) && empty($numRows)) {
            return;
        }

        $dbType = $this->connectionParams['type'];

        if ($dbType === 'sqlsrv') {
            if (is_array($numRows)) {
                $limit = $numRows[1];
                $offset = (int)$numRows[0];
                $orderby = empty($this->orderBy) ? " ORDER BY(SELECT NULL)" : "";
                $this->query .= "$orderby OFFSET $offset ROWS FETCH NEXT $limit ROWS ONLY";
            }
        } else {
            if (is_array($numRows)) {
                $this->query .= ' LIMIT ' . (int) $numRows[1] . ' OFFSET ' . (int) $numRows[0];
            } else {
                $this->query .= ' LIMIT ' . (int) $numRows;
            }
        }
    }

    protected function buildOnDuplicate(?array $tableData): void
    {
        if (is_array($this->updateColumns) && !empty($this->updateColumns)) {
            $this->query .= " ON DUPLICATE KEY UPDATE ";
            if ($this->lastInsertId) {
                $this->query .= $this->lastInsertId . "=LAST_INSERT_ID (" . $this->lastInsertId . "), ";
            }

            foreach ($this->updateColumns as $key => $val) {
                if (is_numeric($key)) {
                    $this->updateColumns[$val] = '';
                    unset($this->updateColumns[$key]);
                } elseif (is_array($tableData)) {
                    $tableData[$key] = $val;
                }
            }
            if (is_array($tableData)) {
                $this->buildDataPairs($tableData, array_keys($this->updateColumns), false);
            }
        }
    }

    private function buildOrderBy(): void
    {
        if (empty($this->orderBy) || $this->isRecordCountQuery) {
            return;
        }

        $this->query .= " ORDER BY ";
        foreach ($this->orderBy as $prop => $value) {
            if (strtolower(str_replace(" ", "", $prop)) === 'rand()') {
                $this->query .= "RAND(), ";
            } else {
                $this->query .= $prop . " " . $value . ", ";
            }
        }

        $this->query = rtrim($this->query, ', ') . " ";
    }

    private function buildPair(string $operator, mixed $value): string
    {
        if (!($value instanceof BaseModel)) {
            $this->params[] = $value;
            return ' ' . $operator . ' ? ';
        }

        $subQuery = $value->getSubQuery();
        if ($subQuery) {
            foreach ($subQuery['params'] as $p) {
                $this->params[] = $p;
            }
            return " " . $operator . " (" . $subQuery['query'] . ") " . ($subQuery['alias'] ?? '');
        }
        return '';
    }

    private function buildQuery(int|array|null $numRows, ?array $tableData = null): ?PDOStatement
    {
        $this->params = [];
        $this->buildInsertQuery($tableData);

        if ($this->queryType !== "INSERT" && $this->queryType !== "REPLACE") {
            $this->buildJoin();
            $this->buildCondition('WHERE', $this->where);
            $this->buildGroupBy();
            $this->buildCondition('HAVING', $this->having);
            $this->buildOrderBy();

            if (!$this->isRecordCountQuery) {
                $this->buildLimit($numRows);
            }

            if ($this->isSubQuery) {
                return null;
            }
        }

        $this->buildOnDuplicate($tableData);
        return $this->prepare();
    }

    private function buildResult(PDOStatement $stmt): mixed
    {
        if ($stmt->columnCount() == 0) {
            return $stmt->rowCount();
        }

        if ($this->useGenerator) {
            return $this->buildResultGenerator($stmt);
        }

        return $stmt->fetchAll($this->returnType);
    }

    private function buildResultGenerator(PDOStatement $stmt): Generator
    {
        while ($row = $stmt->fetch($this->returnType)) {
            yield $row;
        }
    }

    public function checkTransactionStatus(): void
    {
        if (!$this->transaction) {
            return;
        }
        $this->rollback();
    }

    public function commit(): bool
    {
        $result = $this->pdo()->commit();
        $this->transaction = false;
        return $result;
    }

    public function copy(): BaseModel
    {
        $copy = clone $this;
        $copy->pdo = null;
        return $copy;
    }

    public function dec(int|float $num = 1): array
    {
        return ["[I]" => "-" . $num];
    }

    public function delete(string $tableName, int|array|null $numRows = null): bool
    {
        if ($this->isSubQuery) {
            return false;
        }

        $table = $this->prefix . $tableName;

        if (count($this->join)) {
            $this->query = "DELETE " . preg_replace('/.* (.*)/', '$1', $table) . " FROM " . $table;
        } else {
            $this->query = "DELETE FROM " . $table;
        }

        $stmt = $this->buildQuery($numRows);
        if (!$stmt) return false;

        $stmt->execute();
        $this->lastError     = $stmt->errorInfo();
        $this->lastErrorCode = $stmt->errorCode();
        $this->rowCount      = $stmt->rowCount();
        $this->reset();

        return ($this->rowCount > 0);
    }

    private function determineType(mixed $item): int
    {
        return match (gettype($item)) {
            'NULL' => PDO::PARAM_NULL,
            'boolean' => PDO::PARAM_BOOL,
            'integer' => PDO::PARAM_INT,
            'resource' => PDO::PARAM_LOB,
            default => PDO::PARAM_STR,
        };
    }

    public static function getInstance(): ?BaseModel
    {
        return self::$instance;
    }

    public function getLastError(): ?string
    {
        if (!$this->pdo) {
            return "No Database Connection";
        }
        return (!empty($this->lastError[2]) ? (string)$this->lastError[2] : null);
    }

    public function getLastErrorCode(): ?string
    {
        return (string)$this->lastErrorCode;
    }

    public function getLastInsertId(): string|int|false
    {
        return $this->pdo()->lastInsertId();
    }

    public function getLastQuery(): string
    {
        return $this->lastQuery;
    }

    public function getParams(): array
    {
        return $this->params;
    }

    public function getRowCount(): int
    {
        return $this->rowCount;
    }

    public function getSubQuery(): ?array
    {
        if (!$this->isSubQuery) {
            return null;
        }

        $val = [
            'query' => $this->query,
            'params' => $this->params,
            'alias' => $this->connectionParams['host'] ?? 'sub'
        ];
        $this->reset();
        return $val;
    }

    private function getTableName(string $tableName): string
    {
        return strpos($tableName, '.') !== false ? $tableName : $this->prefix . $tableName;
    }

    public function groupBy(string $groupByField): self
    {
        // Sanitize
        $groupByField = preg_replace("/[^-a-z0-9\.\(\),_\*]+/i", '', $groupByField);
        $this->groupBy[] = $groupByField;
        return $this;
    }

    public function has(string $tableName): bool
    {
        $result = $this->getOne($tableName);
        return $result ? true : false;
    }

    public function having(string $havingProp, mixed $havingValue = 'DBNULL', string $operator = '=', string $cond = 'AND'): self
    {
        if (is_array($havingValue) && ($key = key($havingValue)) != "0") {
            $operator    = (string)$key;
            $havingValue = $havingValue[$key];
        }

        if (count($this->having) == 0) {
            $cond = '';
        }

        $this->having[] = array($cond, $havingProp, $operator, $havingValue);
        return $this;
    }

    public function escape(mixed $value): string
    {
        return $this->pdo()->quote((string)$value, $this->determineType($value));
    }

    public function func(string $expr, ?array $bindParams = null): array
    {
        return ["[F]" => [$expr, $bindParams]];
    }

    public function get(string $tableName, int|array|null $numRows = null, string|array $columns = '*'): mixed
    {
        if (empty($columns)) {
            $columns = '*';
        }

        $column = is_array($columns) ? implode(', ', $columns) : $columns;
        $dbType = $this->connectionParams['type'];
        $queryOptions = implode(' ', $this->queryOptions);

        if (in_array('SQL_CALC_FOUND_ROWS', $this->queryOptions) && $dbType !== 'mysql') {
            $queryOptions = str_ireplace('SQL_CALC_FOUND_ROWS', '', $queryOptions);
        }

        $sqlsrvTop = "";
        if (!is_array($numRows) && $dbType === 'sqlsrv' && !empty($numRows)) {
            $sqlsrvTop = " TOP $numRows ";
        }

        $this->query = "SELECT $queryOptions $sqlsrvTop $column FROM " . $this->getTableName($tableName);

        $stmt = $this->buildQuery($numRows);

        if ($this->isSubQuery) {
            return $this;
        }

        if ($stmt) {
            $stmt->execute();
            $this->lastError     = $stmt->errorInfo();
            $this->lastErrorCode = $stmt->errorCode();
            $this->rowCount      = $stmt->rowCount();
        }

        if (in_array('SQL_CALC_FOUND_ROWS', $this->queryOptions)) {
            if ($dbType === 'mysql') {
                $totalStmt = $this->pdo()->query('SELECT FOUND_ROWS()');
                $this->totalCount = (int)$totalStmt->fetchColumn();
            } else {
                $this->isRecordCountQuery = true;
                $this->query = 'SELECT COUNT(*) FROM ' . $this->getTableName($tableName);
                $totalStmt = $this->buildQuery(null);
                if ($totalStmt) {
                    $totalStmt->execute();
                    $this->totalCount = (int)$totalStmt->fetchColumn();
                }
            }
        }

        $result = $stmt ? $this->buildResult($stmt) : [];
        $this->reset();

        return $result;
    }

    public function getOne(string $tableName, string|array $columns = '*'): mixed
    {
        $result = $this->get($tableName, 1, $columns);

        if ($result instanceof BaseModel) {
            return $result;
        }

        if ($this->useGenerator && $result instanceof Generator) {
            return $result->current() ?: false;
        } elseif (is_array($result)) {
            return $result[0] ?? false;
        }
        return false;
    }

    public function getValue(string $tableName, string $column, int $limit = 1): mixed
    {
        $result = $this->setReturnType(PDO::FETCH_ASSOC)->get($tableName, $limit, "{$column} AS retval");

        if (!$result) {
            return null;
        }

        if ($limit == 1) {
            $current = isset($result[0]) ? $result[0] : $result; // Handle if generator logic differs
            if ($result instanceof Generator) $current = $result->current();

            return $current["retval"] ?? null;
        }

        $newRes = [];
        foreach ($result as $current) {
            if ($limit-- <= 0) break;
            $newRes[] = $current['retval'];
        }
        return $newRes;
    }

    public function inc(int|float $num = 1): array
    {
        return ["[I]" => "+" . $num];
    }

    public function insert(string $tableName, array $insertData): int|bool
    {
        return $this->buildInsert($tableName, $insertData, 'INSERT');
    }

    public function insertMulti(string $tableName, array $multiInsertData, ?array $dataKeys = null): array|bool
    {
        $autoCommit = !isset($this->transaction) || !$this->transaction;
        $ids = [];

        if ($autoCommit) {
            $this->startTransaction();
        }

        try {
            foreach ($multiInsertData as $insertData) {
                if ($dataKeys !== null) {
                    $insertData = array_combine($dataKeys, $insertData);
                }

                $id = $this->insert($tableName, $insertData);
                if (!$id) {
                    if ($autoCommit) $this->rollback();
                    return false;
                }
                $ids[] = $id;
            }

            if ($autoCommit) {
                $this->commit();
            }
        } catch (Exception $e) {
            if ($autoCommit) $this->rollback();
            return false;
        }

        return $ids;
    }

    public function loadCsvData(string $csv_path, array $options = [], bool $transaction = true): array
    {
        if (!file_exists($csv_path) || !is_readable($csv_path)) {
            throw new Exception('Cannot open CSV file: ' . $csv_path);
        }

        $delimiter = $options['delimiter'] ?? ',';
        $quote = $options['quote'] ?? '"';
        $table = $options['table'] ?? preg_replace("/[^a-zA-Z0-9_]/i", '', basename($csv_path));

        $handle = fopen($csv_path, "r");
        if (!$handle) throw new Exception("Failed to read CSV");

        if (empty($options['fields'])) {
            $header = fgetcsv($handle, 0, $delimiter, $quote);
            $fields = array_map(function ($field) {
                return strtolower(preg_replace("/[^a-zA-Z0-9_]/i", '', $field));
            }, $header);
            $insert_fields_str = join(', ', $fields);
        } else {
            $fields = $options['fields'];
            $insert_fields_str = is_array($fields) ? join(', ', $fields) : $fields;
        }

        if ($transaction) {
            $this->startTransaction();
        }

        $insert_values_str = join(', ', array_fill(0, count($fields), '?'));
        $insert_sql = "INSERT INTO $table ($insert_fields_str) VALUES ($insert_values_str)";
        $stmt = $this->pdo()->prepare($insert_sql);

        $total_rows = 0;
        try {
            while (($data = fgetcsv($handle, 0, $delimiter, $quote)) !== false) {
                $stmt->execute($data);
                $total_rows++;
            }
            if ($transaction) $this->commit();
        } catch (Exception $e) {
            if ($transaction) $this->rollback();
            fclose($handle);
            throw $e;
        }

        fclose($handle);

        return [
            'table' => $table,
            'fields' => $fields,
            'total_rows' => $total_rows,
        ];
    }

    public function loadJsonData(string $file_path, string $table, bool $transaction = true): array
    {
        if (!file_exists($file_path)) {
            throw new Exception('JSON file not found');
        }

        if ($transaction) {
            $this->startTransaction();
        }

        $jsonstr = file_get_contents($file_path);
        $json = json_decode($jsonstr, true);

        if (!is_array($json) || empty($json)) {
            throw new Exception('Invalid JSON data');
        }

        $fields = array_keys($json[0]);
        $insert_fields_str = join(', ', $fields);
        $insert_values_str = join(', ', array_fill(0, count($fields), '?'));
        $insert_sql = "INSERT INTO $table ($insert_fields_str) VALUES ($insert_values_str)";
        $stmt = $this->pdo()->prepare($insert_sql);
        $total_rows = 0;

        try {
            foreach ($json as $row) {
                $data = array_values($row);
                $stmt->execute($data);
                $total_rows++;
            }
            if ($transaction) $this->commit();
        } catch (Exception $e) {
            if ($transaction) $this->rollback();
            throw $e;
        }

        return [
            'table' => $table,
            'fields' => $fields,
            'total_rows' => $total_rows,
        ];
    }

    public function interval(string $diff, string $func = "NOW()"): string
    {
        $types = ["s" => "second", "m" => "minute", "h" => "hour", "d" => "day", "M" => "month", "Y" => "year"];
        $incr  = '+';
        $items = '';
        $type  = 'd';

        if ($diff && preg_match('/([+-]?) ?([0-9]+) ?([a-zA-Z]?)/', $diff, $matches)) {
            if (!empty($matches[1])) $incr = $matches[1];
            if (!empty($matches[2])) $items = $matches[2];
            if (!empty($matches[3])) $type = $matches[3];

            if (!in_array($type, array_keys($types))) {
                throw new Exception("Invalid interval type in '{$diff}'");
            }

            $func .= " " . $incr . " interval " . $items . " " . $types[$type] . " ";
        }
        return $func;
    }

    public function join(string|BaseModel $joinTable, string $joinCondition, string $joinType = ''): self
    {
        $allowedTypes = ['LEFT', 'RIGHT', 'OUTER', 'INNER', 'LEFT OUTER', 'RIGHT OUTER'];
        $joinType = strtoupper(trim($joinType));

        if ($joinType && !in_array($joinType, $allowedTypes)) {
            throw new Exception('Wrong JOIN type: ' . $joinType);
        }

        if (!is_object($joinTable)) {
            $joinTable = $this->prefix . $joinTable;
        }

        $this->join[] = [$joinType, $joinTable, $joinCondition];

        return $this;
    }

    public function not(?string $col = null): array
    {
        return ["[N]" => (string) $col];
    }

    public function now(?string $diff = null, string $func = "NOW()"): array
    {
        return ["[F]" => [$this->interval($diff ?? '', $func)]];
    }

    public function onDuplicate(array $updateColumns, string|int|null $lastInsertId = null): self
    {
        $this->lastInsertId  = $lastInsertId;
        $this->updateColumns = $updateColumns;
        return $this;
    }

    public function orderBy(string $orderByField, string $orderbyDirection = "DESC", ?array $customFields = null): self
    {
        $allowedDirection = ["ASC", "DESC"];
        $orderbyDirection = strtoupper(trim($orderbyDirection));

        // Stricter sanitization
        $orderByField = preg_replace("/[^-a-z0-9\.\(\),_`\*\'\"]+/i", '', $orderByField);
        $orderByField = preg_replace("/(\`)([`a-zA-Z0-9_]*\.)/", '\1' . $this->prefix . '\2', $orderByField);

        if (empty($orderbyDirection) || !in_array($orderbyDirection, $allowedDirection)) {
            throw new Exception('Wrong order direction: ' . $orderbyDirection);
        }

        if (is_array($customFields)) {
            foreach ($customFields as $key => $value) {
                $customFields[$key] = preg_replace("/[^-a-z0-9\.\(\),_ ]+/i", '', $value);
            }
            $orderByField = 'FIELD (' . $orderByField . ', "' . implode('","', $customFields) . '")';
        }
        $this->orderBy[$orderByField] = $orderbyDirection;
        return $this;
    }

    public function orHaving(string $havingProp, mixed $havingValue = null, ?string $operator = null): self
    {
        return $this->having($havingProp, $havingValue, $operator ?? '=', 'OR');
    }

    public function orWhere(string $whereProp, mixed $whereValue = 'DBNULL', string $operator = '='): self
    {
        return $this->where($whereProp, $whereValue, $operator, 'OR');
    }

    public function paginate(string $table, int $page, mixed $fields = null): array
    {
        $offset = $this->pageLimit * ($page - 1);
        $res = $this->withTotalCount()->get($table, [$offset, $this->pageLimit], $fields ?? '*');
        $this->totalPages = (int)ceil($this->totalCount / $this->pageLimit);
        return is_array($res) ? $res : [];
    }

    public function pdo(): PDO
    {
        if (!$this->pdo) {
            $this->connect();
        }

        if (!$this->pdo) {
            throw new Exception('Cannot connect to db');
        }

        return $this->pdo;
    }

    private function prepare(): ?PDOStatement
    {
        $stmt = $this->pdo()->prepare($this->query);

        if (!$this->isRecordCountQuery) {
            $this->lastQuery = $this->query;
        }

        if (!$stmt instanceof PDOStatement) {
            $this->lastErrorCode = $this->pdo()->errorCode();
            $this->lastError     = $this->pdo()->errorInfo();
            return null;
        }

        foreach ($this->params as $key => $value) {
            $stmt->bindValue(
                is_int($key) ? $key + 1 : ':' . $key,
                $value,
                $this->determineType($value)
            );
        }

        return $stmt;
    }

    public function query(string $query, int|array|null $numRows = null, ?array $params = null): mixed
    {
        $this->query = $query;

        if (is_array($params)) {
            $this->params = $params;
        }

        $stmt = $this->buildQuery($numRows);

        if ($stmt) {
            $stmt->execute();
            $this->lastError     = $stmt->errorInfo();
            $this->lastErrorCode = $stmt->errorCode();
            $result              = $this->buildResult($stmt);
        } else {
            $result = null;
        }
        $this->reset();
        return $result;
    }

    public function rawQuery(string $query, ?array $params = null): mixed
    {
        $this->query = $query;
        if (is_array($params)) {
            $this->params = $params;
        }
        $stmt = $this->prepare();

        if ($stmt) {
            $stmt->execute();
            $this->lastError     = $stmt->errorInfo();
            $this->lastErrorCode = $stmt->errorCode();
            $result              = $this->buildResult($stmt);
        } else {
            $result = null;
        }
        $this->reset();
        return $result;
    }

    public function rawQueryOne(string $query, ?array $params = null): mixed
    {
        $result = $this->rawQuery($query, $params);

        if ($this->useGenerator && $result instanceof Generator) {
            return $result->current() ?: false;
        } elseif (is_array($result)) {
            return $result[0] ?? false;
        }
        return false;
    }

    public function rawQueryValue(string $query, ?array $params = null): mixed
    {
        $result = $this->rawQuery($query, $params);

        if ($this->useGenerator && $result instanceof Generator) {
            if (!$result->current()) return null;
            $firstResult = $result->current();
        } else {
            if (!$result) return null;
            $firstResult = $result[0];
        }

        $key = key($firstResult);

        if (preg_match('/limit\s+1;?$/i', $query)) {
            return $firstResult[$key] ?? null;
        }

        $return = [];
        foreach ($result as $row) {
            $return[] = $row[$key];
        }
        return $return;
    }

    public function replace(string $tableName, array $insertData): int|bool
    {
        return $this->buildInsert($tableName, $insertData, 'REPLACE');
    }

    private function reset(): void
    {
        $this->forUpdate       = false;
        $this->groupBy         = [];
        $this->having          = [];
        $this->join            = [];
        $this->lastInsertId    = "";
        $this->lockInShareMode = false;
        $this->nestJoin        = false;
        $this->orderBy         = [];
        $this->params          = [];
        $this->query           = '';
        $this->queryOptions    = [];
        $this->queryType       = '';
        $this->updateColumns   = [];
        $this->where           = [];
    }

    public function rollback(): bool
    {
        $result            = $this->pdo()->rollback();
        $this->transaction = false;
        return $result;
    }

    public function setPageLimit(int $limit): self
    {
        $this->pageLimit = $limit;
        return $this;
    }

    public function setPrefix(string $prefix = ''): self
    {
        $this->prefix = $prefix;
        return $this;
    }

    public function setQueryOption(string|array $options): self
    {
        $allowedOptions = [
            'ALL',
            'DISTINCT',
            'DISTINCTROW',
            'HIGH_PRIORITY',
            'STRAIGHT_JOIN',
            'SQL_SMALL_RESULT',
            'SQL_BIG_RESULT',
            'SQL_BUFFER_RESULT',
            'SQL_CACHE',
            'SQL_NO_CACHE',
            'SQL_CALC_FOUND_ROWS',
            'LOW_PRIORITY',
            'IGNORE',
            'QUICK',
            'MYSQLI_NESTJOIN',
            'FOR UPDATE',
            'LOCK IN SHARE MODE'
        ];

        if (!is_array($options)) {
            $options = [$options];
        }

        foreach ($options as $option) {
            $option = strtoupper($option);
            if (!in_array($option, $allowedOptions)) {
                throw new Exception('Wrong query option: ' . $option);
            }

            if ($option == 'MYSQLI_NESTJOIN') {
                $this->nestJoin = true;
            } elseif ($option == 'FOR UPDATE') {
                $this->forUpdate = true;
            } elseif ($option == 'LOCK IN SHARE MODE') {
                $this->lockInShareMode = true;
            } else {
                $this->queryOptions[] = $option;
            }
        }
        return $this;
    }

    public function setReturnType(int $returnType): self
    {
        $this->returnType = $returnType;
        return $this;
    }

    public function startTransaction(): void
    {
        $this->pdo()->beginTransaction();
        $this->transaction = true;
        // Check transaction status on shutdown
        register_shutdown_function([$this, "checkTransactionStatus"]);
    }

    public function subQuery(string $subQueryAlias = ""): BaseModel
    {
        $params = $this->connectionParams;
        $params['host'] = $subQueryAlias;
        $params['isSubQuery'] = true;
        $params['prefix'] = $this->prefix;

        return new self($params);
    }

    public function tableExists(string|array $tables): bool
    {
        $tables = !is_array($tables) ? [$tables] : $tables;
        $count  = count($tables);
        if ($count == 0) {
            return false;
        }

        foreach ($tables as $i => $value) {
            $tables[$i] = $this->prefix . $value;
        }
        $this->withTotalCount();
        $this->where('table_schema', $this->connectionParams['dbname']);
        $this->where('table_name', $tables, 'in');
        $this->get('information_schema.tables', $count);
        return $this->totalCount == $count;
    }

    public function update(string $tableName, array $tableData, int|array|null $numRows = null): bool
    {
        if ($this->isSubQuery) {
            return false;
        }

        $this->query     = 'UPDATE ' . $this->getTableName($tableName);
        $this->queryType = 'UPDATE';

        $stmt = $this->buildQuery($numRows, $tableData);
        if (!$stmt) return false;

        $status = $stmt->execute();
        $this->lastError     = $stmt->errorInfo();
        $this->lastErrorCode = $stmt->errorCode();
        $this->reset();
        $this->rowCount      = $stmt->rowCount();
        return $status;
    }

    public function useGenerator(bool $option): void
    {
        $this->useGenerator = $option;
    }

    public function where(string $whereProp, mixed $whereValue = 'DBNULL', string $operator = '=', string $cond = 'AND'): self
    {
        if (count($this->where) == 0) {
            $cond = '';
        }

        $this->where[] = [$cond, $whereProp, $operator, $whereValue];
        return $this;
    }

    public function withTotalCount(): self
    {
        $this->setQueryOption('SQL_CALC_FOUND_ROWS');
        return $this;
    }
}
