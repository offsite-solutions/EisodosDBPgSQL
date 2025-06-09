<?php /** @noinspection DuplicatedCode SpellCheckingInspection PhpUnusedFunctionInspection NotOptimalIfConditionsInspection */
  
  namespace Eisodos\Connectors;
  
  use Eisodos\Eisodos;
  use Eisodos\Interfaces\DBConnectorInterface;
  use Exception;
  use RuntimeException;
  
  /**
   * Eisodos PgSQL Connector class
   *
   *
   * Config values:
   * [Database]
   * connectMode=cached|persistent|empty(default)
   * username=
   * password=
   * connection=tnsnev|server:port/SID|tns description
   * characterSet=if empty NLS_LANG environment variable will be used, ex: AL32UTF8
   * autoCommit=true|false(default)
   * connectSQL=list of query run after connection separated by ;
   */
  class ConnectorPgSQL implements DBConnectorInterface {
    
    /** @var resource Connection resource */
    private $_connection;
    
    /** @var bool In transaction flag */
    private bool $_inTransaction;
    
    /** @var array Last query column names */
    private array $_lastQueryColumnNames = [];
    
    /** @var int Last query total rows */
    private int $_lastQueryTotalRows = 0;
    
    /** @var bool Auto commit enabled */
    private bool $_autoCommit = false;
    
    public function __destruct() {
      $this->disconnect();
    }
    
    /** Calling OCI bind based on bound variables
     * @param resource $statement_ OCI statement resource
     * @param array $boundVariables_ Bound variables array structure
     * @return void
     */
    private function _bindVariables($statement_, &$boundVariables_): void {
      
      foreach ($boundVariables_ as $variableName => &$variableProperties) {
        // get OCI constant by incoming data type
        $variableProperties['bindType'] = $this->_convertType($variableProperties['type'], $variableProperties['value']);
        // get bind variable length by data type
        $variableProperties['bindLength'] = $this->_convertLength($variableProperties['type']);
        
        // TODO cursor, rowid, file not yet supported
        // boundValue will be overwritten in case of OUT, IN_OUT parameters
        if ($variableProperties['bindType'] === OCI_B_CLOB) {
          $variableProperties['descriptor'] = oci_new_descriptor($this->_connection, OCI_D_LOB);
          $variableProperties['boundValue'] = &$variableProperties['descriptor']; // important to assign as reference
          $variableProperties['bindLength'] = -1;
        } else {
          $variableProperties['descriptor'] = false;
          $variableProperties['boundValue'] = $variableProperties['value'];
          $variableProperties['value'] = '';
        }
        
        Eisodos::$logger->trace(
          'Binding variable: ' .
          $variableName . ' - ' .
          ($variableProperties['descriptor'] !== false ? "OCILob object" : ((strlen($variableProperties['boundValue']) > 255) ? ("(" . mb_strlen($variableProperties['boundValue']) . " bytes of data)") : $variableProperties['boundValue'])) . " - " .
          $variableProperties['bindType'] . ' - ' .
          $variableProperties['bindLength']);
        
        // bindig OCI to variables
        oci_bind_by_name(
          $statement_,
          ':' . $variableName,
          $variableProperties['boundValue'], // reference, value will be overwritten
          $variableProperties['bindLength'],
          $variableProperties['bindType']
        );
        
        // In case of descriptors, value is written after binding
        if ($variableProperties['descriptor'] !== false && str_contains($variableProperties['mode_'], 'IN')) {
          if ($variableProperties['descriptor']->writeTemporary($variableProperties['value'])) {
            Eisodos::$logger->trace('  OCI LOB descriptor for parameter ' . $variableName . ' written with ' . $variableProperties['descriptor']->size() . " bytes\n");
          } else {
            throw new RuntimeException('Could not write temporary LOB for parameter ' . $variableName);
          }
        }
        
      }
    }
    
    /** Free up descriptors, fill value with values given back in out parameters
     * @param array $boundVariables_ Bound variables
     * @return void
     */
    private function _freeVariables(&$boundVariables_): void {
      
      foreach ($boundVariables_ as &$variableProperties) {
        
        // free OCILob objects
        if ($variableProperties['descriptor'] !== false) {
          oci_free_descriptor($variableProperties['descriptor']);
          $variableProperties['descriptor'] = false;
        }
        
        // value is overwritten by outgoing values in case of OUT, IN_OUT parameters
        if (str_contains($variableProperties['mode_'], 'OUT')) {
          $variableProperties['value'] = $variableProperties['boundValue'];
          $variableProperties['boundValue'] = '';
        }
      }
    }
    
    /** OCI execute routine
     * @param resource $SQL_ SQL sentence
     * @param string $exceptionMessage_ Exception message, if filled exception threw in case of error
     * @return bool|mixed
     */
    private function &_execute($SQL_, $exceptionMessage_ = '') {
      $transactionMode = $this->inTransaction() ? OCI_NO_AUTO_COMMIT : OCI_COMMIT_ON_SUCCESS;
      $result = pg_query($this->_connection, $SQL_);
      if ($result === false) {
        $e = pg_last_error($this->_connection);
        // Eisodos::$logger->writeErrorLog(NULL, 'Could not execute statement - ' . $e['code'] . ' - ' . $e['message'] . "\n" . $e['sqltext']);
        Eisodos::$parameterHandler->setParam('DBError', 'Could not execute statement - ' . $e['message'] . "\n" . $SQL_);
        if (!$exceptionMessage_) {
          $_POST["__EISODOS_extendedError"] = 'Could not execute statement - ' . $e['message'] . "\n" . $SQL_;
          throw new RuntimeException('Could not execute statement');
        }
      }
      
      return $result;
    }
    
    /** OCI parse given SQL command
     * @param string $SQL_ SQL command
     * @param string $exceptionMessage_ Exception message, if filled exception threw in case of error
     * @return false|mixed|resource
     */
    private function &_parse($SQL_, $exceptionMessage_ = '') {
      
      $statement = oci_parse($this->_connection, $SQL_);
      if ($statement === false) {
        $e = oci_error();
        Eisodos::$parameterHandler->setParam('DBError', 'Could not parse query - ' . $e['code'] . ' - ' . $e['message'] . "\n" . $SQL_);
        if (!$exceptionMessage_) {
          $_POST['__EISODOS_extendedError'] = 'Could not parse query - ' . $e['code'] . ' - ' . $e['message'] . "\n" . $SQL_;
          throw new RuntimeException('Could not parse query');
        }
      }
      
      return $statement;
    }
    
    /** Check connection, throws exception when lost
     * @return void
     * @throws RuntimeException
     */
    private function _checkConnection(): void {
      if (!$this->connected()) {
        throw new RuntimeException('Database connection not established!');
      }
    }
    
    /**
     * @inheritDoc
     */
    public function connected(): bool {
      return !empty($this->_connection);
    }
    
    /**
     * @inheritDoc
     * @throws RuntimeException|Exception
     */
    public function connect($databaseConfigSection_ = 'Database', $connectParameters_ = [], $persistent_ = false): void {
      if (!isset($this->_connection)) {
        $databaseConfig = array_change_key_case(Eisodos::$configLoader->importConfigSection($databaseConfigSection_, '', false));
        
        $connectString = '';
        
        if (Eisodos::$utils->safe_array_value($databaseConfig, 'user') !== '') {
          $connectString .= ' user=' . Eisodos::$utils->safe_array_value($databaseConfig, 'user');
        }
        if (Eisodos::$utils->safe_array_value($databaseConfig, 'password') !== '') {
          $connectString .= ' password=' . Eisodos::$utils->safe_array_value($databaseConfig, 'password');
        }
        if (Eisodos::$utils->safe_array_value($databaseConfig, 'host') !== '') {
          $connectString .= ' host=' . Eisodos::$utils->safe_array_value($databaseConfig, 'host');
        }
        if (Eisodos::$utils->safe_array_value($databaseConfig, 'hostaddr') !== '') {
          $connectString .= ' hostaddr=' . Eisodos::$utils->safe_array_value($databaseConfig, 'hostaddr');
        }
        if (Eisodos::$utils->safe_array_value($databaseConfig, 'port') !== '') {
          $connectString .= ' port=' . Eisodos::$utils->safe_array_value($databaseConfig, 'port');
        }
        if (Eisodos::$utils->safe_array_value($databaseConfig, 'dbname') !== '') {
          $connectString .= ' dbname=' . Eisodos::$utils->safe_array_value($databaseConfig, 'dbname');
        }
        if (Eisodos::$utils->safe_array_value($databaseConfig, 'connect_timeout') !== '') {
          $connectString .= ' connect_timeout=' . Eisodos::$utils->safe_array_value($databaseConfig, 'connect_timeout');
        }
        if (Eisodos::$utils->safe_array_value($databaseConfig, 'requiressl') !== '') {
          $connectString .= ' requiressl=' . Eisodos::$utils->safe_array_value($databaseConfig, 'requiressl');
        }
        if (Eisodos::$utils->safe_array_value($databaseConfig, 'service') !== '') {
          $connectString .= ' service=' . Eisodos::$utils->safe_array_value($databaseConfig, 'service');
        }
        if (Eisodos::$utils->safe_array_value($databaseConfig, 'options') !== '') {
          $connectString .= " options='" . Eisodos::$utils->safe_array_value($databaseConfig, 'options') . "'";
        }
        
        if ($persistent_) {
          $connection = @pg_pconnect(
            $connectString
          );
        } else {
          $connection = @pg_connect(
            $connectString
          );
        }
        
        if (!$connection) {
          $e = error_get_last();
          Eisodos::$parameterHandler->setParam('DBError', $e['message']);
          throw new RuntimeException('Database Open Error!');
        }
        
        $this->_connection = $connection;
        
        $this->_autoCommit = (Eisodos::$utils->safe_array_value($databaseConfig, 'autocommit') === 'true');
        
        Eisodos::$logger->trace('Database connected - ' . pg_dbname($this->_connection) . ' - ' . $connectString);
        
        $connectSQL = Eisodos::$utils->safe_array_value($databaseConfig, 'connectsql');
        
        if (!$this->_autoCommit) {
          $this->_inTransaction = true;
        } else {
          $connectSQL = "set autocommit to ON;" . $connectSQL;
        }
        
        /*
        if (stripos($connectSQL, 'set datestyle') === false) {
          $connectSQL = "set datestyle to ISO;" . $connectSQL;
        }
        
        foreach (explode(';', $connectSQL) as $sql) {
          if ($sql !== '') {
            $this->query(RT_FIRST_ROW_FIRST_COLUMN, $sql);
          }
        }*/
        
      }
      
    }
    
    /**
     * @inheritDoc
     */
    public function disconnect($force_ = false): void {
      if ($this->connected()) {
        if ($this->inTransaction()) {
          $this->query(RT_RAW, 'ROLLBACK');
        }
        pg_close($this->_connection);
        $this->_connection = NULL;
        Eisodos::$logger->trace('Database disconnected');
      }
    }
    
    /**
     * @inheritDoc
     */
    public function startTransaction($savePoint_ = NULL): void {
      $this->_checkConnection();
      
      if (!$this->inTransaction()) {
        if ($savePoint_ !== NULL) {
          $this->query(RT_RAW, 'SAVEPOINT ' . $savePoint_);
        } else {
          $this->query(RT_RAW, 'START TRANSACTION');
        }
        $this->_inTransaction = true;
      } else {
        throw new RuntimeException('Transaction already started!');
      }
    }
    
    /**
     * @inheritDoc
     */
    public function commit(): void {
      $this->_checkConnection();
      
      if ($this->inTransaction()) {
        $this->query(RT_RAW, 'COMMIT');
        Eisodos::$logger->trace('Transaction committed');
        $this->_inTransaction = true;
      }
    }
    
    /**
     * @inheritDoc
     * @throws Exception
     */
    public function rollback($savePoint_ = NULL): void {
      $this->_checkConnection();
      
      if ($this->inTransaction()) {
        if ($savePoint_ !== NULL) {
          $this->query(RT_RAW, 'ROLLBACK TO SAVEPOINT ' . $savePoint_);
          Eisodos::$logger->trace('Transaction rolled back to savepoint: ' . $savePoint_);
        } else {
          $this->query(RT_RAW, 'ROLLBACK');
          Eisodos::$logger->trace('Transaction rolled back');
        }
        $this->_inTransaction = true;
      }
    }
    
    /**
     * @inheritDoc
     */
    public function inTransaction(): bool {
      $this->_checkConnection();
      
      return !$this->_autoCommit && $this->_inTransaction;
    }
    
    /**
     * @inheritDoc
     */
    public function query(int $resultTransformation_, string $SQL_, &$queryResult_ = NULL, $getOptions_ = [], $exceptionMessage_ = '') {
      $this->_lastQueryColumnNames = [];
      $this->_lastQueryTotalRows = 0;
      
      $this->_checkConnection();
      
      Eisodos::$logger->trace("Running query: \n" . $SQL_);
      
      $executeResult = $this->_execute($statement, $exceptionMessage_);
      if ($executeResult === false) {
        if ($statement) {
          oci_free_statement($statement);
        }
        
        return false;
      }
      
      
      for ($i = 1; $i <= oci_num_fields($statement); $i++) {
        $this->_lastQueryColumnNames[] = oci_field_name($statement, $i);
      }
      
      $rows = [];
      
      if ($resultTransformation_ === RT_RAW) {
        oci_fetch_all($statement, $rows, 0, -1, OCI_FETCHSTATEMENT_BY_ROW + OCI_ASSOC);
        if ($statement) {
          oci_free_statement($statement);
        }
        if (!$rows) {
          return false;
        }
        
        $queryResult_ = $rows;
        $this->_lastQueryTotalRows = count($rows);
        
        return true;
      }
      
      if ($resultTransformation_ === RT_FIRST_ROW) {
        oci_fetch_all($statement, $rows, 0, 1, OCI_FETCHSTATEMENT_BY_ROW + OCI_ASSOC);
        if ($statement) {
          oci_free_statement($statement);
        }
        if (!$rows || count($rows) === 0) {
          return false;
        }
        
        $queryResult_ = $rows[0];
        $this->_lastQueryTotalRows = 1;
        
        return true;
      }
      
      if ($resultTransformation_ === RT_FIRST_ROW_FIRST_COLUMN) {
        oci_fetch_all($statement, $rows, 0, 1, OCI_FETCHSTATEMENT_BY_ROW + OCI_NUM);
        if ($statement) {
          oci_free_statement($statement);
        }
        if (!$rows || count($rows) === 0) {
          return '';
        }
        
        $this->_lastQueryTotalRows = 1;
        
        return $rows[0][0];
      }
      
      if ($resultTransformation_ === RT_ALL_KEY_VALUE_PAIRS
        || $resultTransformation_ === RT_ALL_FIRST_COLUMN_VALUES
        || $resultTransformation_ === RT_ALL_ROWS
        || $resultTransformation_ === RT_ALL_ROWS_ASSOC) {
        
        if ($resultTransformation_ === RT_ALL_KEY_VALUE_PAIRS) {
          while ($row = oci_fetch_array($statement, OCI_NUM + OCI_RETURN_NULLS + OCI_RETURN_LOBS)) {
            $queryResult_[$row[0]] = $row[1];
          }
        } else if ($resultTransformation_ === RT_ALL_FIRST_COLUMN_VALUES) {
          while ($row = oci_fetch_array($statement, OCI_NUM + OCI_RETURN_NULLS + OCI_RETURN_LOBS)) {
            $queryResult_[] = $row[0];
          }
        } else if ($resultTransformation_ === RT_ALL_ROWS) {
          oci_fetch_all($statement, $queryResult_, 0, -1, OCI_FETCHSTATEMENT_BY_ROW + OCI_ASSOC);
        } else if ($resultTransformation_ === RT_ALL_ROWS_ASSOC) {
          $indexFieldName = Eisodos::$utils->safe_array_value($getOptions_, 'indexFieldName', false);
          if (!$indexFieldName) {
            throw new RuntimeException('Index field name is mandatory on RT_ALL_ROWS_ASSOC result type');
          }
          while ($row = oci_fetch_assoc($statement)) {
            $queryResult_[$row[$indexFieldName]] = $row;
          }
        }
        
        if ($statement) {
          oci_free_statement($statement);
        }
        
        $this->_lastQueryTotalRows = count($queryResult_);
        
        return true;
      }
      
      throw new RuntimeException('Unknown query result type');
      
    }
    
    /**
     * @inheritDoc
     * @throws Exception
     */
    public function executeDML(string $SQL_, $throwException_ = true): int|bool {
      $this->_checkConnection();
      
      Eisodos::$logger->trace("Executing DML: \n" . $SQL_);
      
      $statement = false;
      
      try {
        
        $statement = $this->_parse($SQL_, $throwException_ ? 'DML Exception' : '');
        if ($statement === false) {
          
          return false;
        }
        $executeResult = $this->_execute($statement, $throwException_ ? 'DML Exception' : '');
        if ($executeResult === false) {
          if ($statement) {
            oci_free_statement($statement);
          }
          
          return false;
        }
        
      } catch (Exception $e) {
        if ($statement) {
          oci_free_statement($statement);
        }
        throw $e;
      }
      
      $numRows = oci_num_rows($statement);
      Eisodos::$logger->trace('Number of rows modified: ' . $numRows);
      if ($statement) {
        oci_free_statement($statement);
      }
      
      return $numRows;
    }
    
    /**
     * @inheritDoc
     */
    public function bind(array &$boundVariables_, string $variableName_, string $dataType_, string $value_, $inOut_ = 'IN'): void {
      $boundVariables_[$variableName_] = array();
      if ($dataType_ === 'clob' && $value_ === '') // Empty CLOB bug / invalid LOB locator specified, force type to text
      {
        $boundVariables_[$variableName_]['type'] = 'text';
      } else {
        $boundVariables_[$variableName_]['type'] = $dataType_;
      }
      $boundVariables_[$variableName_]['value'] = $value_;
      // TODO rename to mode
      $boundVariables_[$variableName_]['mode_'] = $inOut_;
    }
    
    /**
     * @inheritDoc
     */
    public function bindParam(array &$boundVariables_, string $parameterName_, string $dataType_): void {
      $this->bind($boundVariables_, $parameterName_, $dataType_, Eisodos::$parameterHandler->getParam($parameterName_));
    }
    
    /**
     * @inheritDoc
     */
    public function executePreparedDML(string $SQL_, $dataTypes_ = [], $data_ = [], $throwException_ = true): int|bool {
      throw new RuntimeException('Execute prepared DML not supported! Use executePreparedDML2()!');
    }
    
    /**
     * @inheritDoc
     * Bind variables: ['variableName'=>['value'=>'','type'=>'','lenght'=>'','mode_'=>'IN|IN_OUT|OUT']]
     * SQL: insert into tablename (id,column1,column2) values (:variableName,:variableName2,:variableName3)
     * @throws Exception
     */
    public function executePreparedDML2(string $SQL_, array $boundVariables_, $throwException_ = true): int|bool {
      $this->_checkConnection();
      
      Eisodos::$logger->trace("Executing DML: \n" . $SQL_);
      
      $statement = $this->_parse($SQL_, $throwException_ ? 'DML Exception' : '');
      if ($statement === false) {
        
        return false;
      }
      
      /* bindig variables */
      $this->_bindVariables($statement, $boundVariables_);
      
      try {
        $executeResult = $this->_execute($statement, $throwException_ ? 'DML Exception' : '');
        if ($executeResult === false) {
          $this->_freeVariables($boundVariables_);
          if ($statement) {
            oci_free_statement($statement);
          }
          
          return false;
        }
      } catch (Exception $e) {
        $this->_freeVariables($boundVariables_);
        if ($statement) {
          oci_free_statement($statement);
        }
        throw $e;
      }
      
      $numRows = oci_num_rows($statement);
      Eisodos::$logger->trace('Number of rows modified: ' . $numRows);
      $this->_freeVariables($boundVariables_);
      if ($statement) {
        oci_free_statement($statement);
      }
      
      return $numRows;
    }
    
    /**
     * @inheritDoc
     * @throws Exception
     */
    public function executeStoredProcedure(string $procedureName_, array $inputVariables_, array &$resultVariables_, $throwException_ = true, $case_ = CASE_UPPER): bool {
      $this->_checkConnection();
      
      // keep input variables in the result array
      $resultVariables_ = $inputVariables_;
      
      // generate stored procedure sql command from incoming parameters
      $sql = '';
      foreach ($resultVariables_ as $parameterName => $parameterProperties) {
        $sql .= ($sql ? ',' : '') . $parameterName . ' => :' . $parameterName;
      }
      $sql = 'BEGIN ' . $procedureName_ . '(' . $sql . '); END; ';
      
      Eisodos::$logger->trace("Executing stored procedure: \n" . $sql);
      
      $statement = $this->_parse($sql, $throwException_ ? 'Stored Procedure Exception' : '');
      if ($statement === false) {
        
        return false;
      }
      
      $this->_bindVariables($statement, $resultVariables_);
      
      try {
        $executeResult = $this->_execute($statement, $throwException_ ? 'Stored Procedure Exception' : '');
        
        $this->_freeVariables($resultVariables_);
        if ($statement) {
          oci_free_statement($statement);
        }
        if ($executeResult === false) {
          
          return false;
        }
        
      } catch (Exception $e) {
        $this->_freeVariables($resultVariables_);
        if ($statement) {
          oci_free_statement($statement);
        }
        throw $e;
      }
      
      return true;
    }
    
    /**
     * @inheritDoc
     */
    public function getLastQueryColumns(): array {
      return $this->_lastQueryColumnNames;
    }
    
    /**
     * @inheritDoc
     */
    public function getLastQueryTotalRows(): int {
      return $this->_lastQueryTotalRows;
    }
    
    /**
     * @inheritDoc
     */
    public function getConnection() {
      return $this->_connection;
    }
    
    /**
     * @inheritDoc
     */
    public function emptySQLField($value_, $isString_ = true, $maxLength_ = 0, $exception_ = '', $withComma_ = false, $keyword_ = 'NULL'): string {
      if ($value_ === '') {
        if ($withComma_) {
          return 'NULL, ';
        }
        
        return 'NULL';
      }
      if ($isString_) {
        if ($maxLength_ > 0 && mb_strlen($value_, 'UTF-8') > $maxLength_) {
          if ($exception_) {
            throw new RuntimeException($exception_);
          }
          
          $value_ = substr($value_, 0, $maxLength_);
        }
        $result = "'" . Eisodos::$utils->replace_all($value_, "'", "''") . "'";
      } else {
        $result = $value_;
      }
      if ($withComma_) {
        $result .= ', ';
      }
      
      return $result;
    }
    
    /**
     * @inheritDoc
     */
    public function nullStr($value_, $isString_ = true, $maxLength_ = 0, $exception_ = '', $withComma_ = false): string {
      return $this->emptySQLField($value_, $isString_, $maxLength_, $exception_, $withComma_);
    }
    
    /**
     * @inheritDoc
     */
    public function defaultStr($value_, $isString_ = true, $maxLength_ = 0, $exception_ = '', $withComma_ = false): string {
      return $this->emptySQLField($value_, $isString_, $maxLength_, $exception_, $withComma_, 'DEFAULT');
    }
    
    /**
     * @inheritDoc
     */
    public function nullStrParam(string $parameterName_, $isString_ = true, $maxLength_ = 0, $exception_ = '', $withComma_ = false): string {
      return $this->emptySQLField(Eisodos::$parameterHandler->getParam($parameterName_), $isString_, $maxLength_, $exception_, $withComma_);
    }
    
    /**
     * @inheritDoc
     */
    public function defaultStrParam(string $parameterName_, $isString_ = true, $maxLength_ = 0, $exception_ = '', $withComma_ = false): string {
      return $this->emptySQLField(Eisodos::$parameterHandler->getParam($parameterName_), $isString_, $maxLength_, $exception_, $withComma_, 'DEFAULT');
    }
    
  }