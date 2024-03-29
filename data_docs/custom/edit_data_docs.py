from datetime import datetime
import re

def edit_data_docs(data_docs_input_name, table_name, data_docs_output_name=None):
    
    if data_docs_output_name is None:
        data_docs_output_name = data_docs_input_name
        
    f = open(data_docs_input_name, 'r', encoding="utf-8")
    data_docs = f.read()
    f.close()
    
    today = datetime.now()

    data_docs = data_docs.replace('Overview', 'Overview: %s' % table_name)
    data_docs = re.sub(r'Data asset: \w+<br>', '<b>Date: %s</b><br>' % today.strftime('%d/%m/%Y %H:%M'), data_docs)
    data_docs = data_docs.replace("""<div class="mb-2">
          <div class="d-flex justify-content-center">
            <button type="button" class="btn btn-warning" data-toggle="modal" data-target=".ge-expectation-editing-instructions-modal">
              <i class="fas fa-edit"></i> How to Edit This Suite
            </button>
          </div>
        </div>
      
      <div class="mb-2">
        <div class="d-flex justify-content-center">
          <button type="button" class="btn btn-info" data-toggle="modal" data-target=".ge-walkthrough-modal">
            Show Walkthrough
          </button>
        </div>
      </div>""", '')
    
    
    # Tira o pop-up de "ajuda" quando renderiza a página
    data_docs = data_docs.replace("""<div class="modal fade ge-walkthrough-modal" tabindex="-1" role="dialog" aria-labelledby="ge-walkthrough-modal-title"
     aria-hidden="true">
  <div class="modal-dialog modal-dialog-centered modal-lg">
    <div class="modal-content">
      <div class="modal-header">
        <h6 class="modal-title" id="ge-walkthrough-modal-title">Great Expectations Walkthrough</h6>
        <button type="button" class="close" data-dismiss="modal" aria-label="Close">
          <span aria-hidden="true">&times;</span>
        </button>
      </div>
      <div class="modal-body">
        <div class="card walkthrough-card bg-dark text-white walkthrough-1">
        <div class="card-header"><h4>How to work with Great Expectations</h4></div>
            <div class="card-body">
              <div class="image-sizer text-center">
                  <img src="../../../../../static/images/iterative-dev-loop.png" class="img-fluid rounded-sm mx-auto dev-loop">
              </div>
                  <p>
                      Welcome! Now that you have initialized your project, the best way to work with Great Expectations is in this iterative dev loop:
                  </p>
                <ol>
                    <li>Let Great Expectations create a (terrible) first draft suite, by running <code class="inline-code bg-light">great_expectations suite new</code>.</li>
                    <li>View the suite here in Data Docs.</li>
                    <li>Edit the suite in a Jupyter notebook by running <code class="inline-code bg-light">great_expectations suite edit</code></li>
                    <li>Repeat Steps 2-3 until you are happy with your suite.</li>
                    <li>Commit this suite to your source control repository.</li>
                </ol>
            </div>
            <div class="card-footer walkthrough-links">
              <div class="container">
                <div class="row">
                  <div class="col-sm">
                    &nbsp;
                  </div>
                  <div class="col-6 text-center text-secondary">
                    1 of 7
                    <div class="progress" style="height: 2px">
                      <div class="progress-bar bg-info" role="progressbar" style="width: 16%" aria-valuenow="16" aria-valuemin="0" aria-valuemax="16"></div>
                    </div>
                  </div>
                  <div class="col-sm">
                    <a href="#" class="next-link btn btn-primary float-right" onclick="go_to_slide(2)">Next</a>
                  </div>
                </div>
              </div>
            </div>
        </div>
                <div class="card walkthrough-card bg-dark text-white walkthrough-2">
                <div class="card-header"><h4>What are Expectations?</h4></div>
                <div class="card-body">
                    <ul class="code-snippet bg-light text-muted rounded-sm">
                        <li>expect_column_to_exist</li>
                        <li>expect_table_row_count_to_be_between</li>
                        <li>expect_column_values_to_be_unique</li>
                        <li>expect_column_values_to_not_be_null</li>
                        <li>expect_column_values_to_be_between</li>
                        <li>expect_column_values_to_match_regex</li>
                        <li>expect_column_mean_to_be_between</li>
                        <li>expect_column_kl_divergence_to_be_less_than</li>
                        <li>... <a href="https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=walkthrough&utm_medium=glossary">and many more</a></li>
                    </ul>
                  <p>An expectation is a falsifiable, verifiable statement about data.</p>
                  <p>Expectations provide a language to talk about data characteristics and data quality - humans to humans, humans to machines and machines to machines.</p>
                  <p>Expectations are both data tests and docs!</p>
                </div>
                <div class="card-footer walkthrough-links">
                  <div class="container">
                    <div class="row">
                      <div class="col-sm">
                        <a href="#" class="next-link btn btn-secondary float-left" onclick="go_to_slide(1)">Back</a>
                      </div>
                      <div class="col-6 text-center text-secondary">
                        2 of 7
                        <div class="progress" style="height: 2px">
                          <div class="progress-bar bg-info" role="progressbar" style="width: 32%" aria-valuenow="32" aria-valuemin="0" aria-valuemax="32"></div>
                        </div>
                      </div>
                      <div class="col-sm">
                        <a href="#" class="next-link btn btn-primary float-right" onclick="go_to_slide(3)">Next</a>
                      </div>
                    </div>
                  </div>
                </div>
                </div>
                <div class="card walkthrough-card bg-dark text-white walkthrough-3">
                <div class="card-header"><h4>Expectations can be presented in a machine-friendly JSON</h4></div>
                <div class="card-body">
                    <p class="code-snippet bg-light text-muted rounded-sm">
{<br />
&nbsp;&nbsp;&nbsp;&nbsp;<span class="json-key">"expectation_type"</span>: <span class="json-str">"expect_column_values_to_not_be_null",</span><br />
&nbsp;&nbsp;&nbsp;&nbsp;<span class="json-key">"kwargs"</span>: {<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="json-key">"column"</span>: <span class="json-str">"user_id"</span><br />
&nbsp;&nbsp;&nbsp;&nbsp;}<br />
}<br />
                    </p>
                  <p>A machine can test if a dataset conforms to the expectation.</p>
                </div>
                <div class="card-footer walkthrough-links">
                  <div class="container">
                    <div class="row">
                      <div class="col-sm">
                        <a href="#" class="next-link btn btn-secondary float-left" onclick="go_to_slide(2)">Back</a>
                      </div>
                      <div class="col-6 text-center text-secondary">
                        3 of 7
                        <div class="progress" style="height: 2px">
                          <div class="progress-bar bg-info" role="progressbar" style="width: 50%" aria-valuenow="50" aria-valuemin="0" aria-valuemax="50"></div>
                        </div>
                      </div>
                      <div class="col-sm">
                        <a href="#" class="next-link btn btn-primary float-right" onclick="go_to_slide(4)">Next</a>
                      </div>
                    </div>
                  </div>
                </div>
                </div>
                <div class="card walkthrough-card bg-dark text-white walkthrough-4">
                <div class="card-header"><h4>Validation produces a validation result object</h4></div>
                <div class="card-body">
                     <p class="code-snippet bg-light text-muted rounded-sm">
{<br />
&nbsp;&nbsp;&nbsp;&nbsp;<span class="json-key">"success"</span>: <span class="json-bool">false</span>,<br />
&nbsp;&nbsp;&nbsp;&nbsp;<span class="json-key">"result":</span> {<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="json-key">"element_count"</span>: <span class="json-number">253405,</span><br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="json-key">"unexpected_count"</span>: <span class="json-number">7602,</span><br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="json-key">"unexpected_percent"</span>: <span class="json-number">2.999</span><br />
&nbsp;&nbsp;&nbsp;&nbsp;},<br />
&nbsp;&nbsp;&nbsp;&nbsp;<span class="json-key">"expectation_config"</span>: {<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="json-key">"expectation_type"</span>: <span class="json-str">"expect_column_values_to_not_be_null"</span>,<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="json-key">"kwargs"</span>: {<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="json-key">"column"</span>: <span class="json-str">"user_id"</span><br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;}<br />
}<br />
                    </p>
                  <p>Here's an example Validation Result (not from your data) in JSON format. This object has rich context about the test failure.</p>
                </div>
                <div class="card-footer walkthrough-links">
                  <div class="container">
                    <div class="row">
                      <div class="col-sm">
                        <a href="#" class="next-link btn btn-secondary float-left" onclick="go_to_slide(3)">Back</a>
                      </div>
                      <div class="col-6 text-center text-secondary">
                        4 of 7
                        <div class="progress" style="height: 2px">
                          <div class="progress-bar bg-info" role="progressbar" style="width: 68%" aria-valuenow="68" aria-valuemin="0" aria-valuemax="68"></div>
                        </div>
                      </div>
                      <div class="col-sm">
                        <a href="#" class="next-link btn btn-primary float-right" onclick="go_to_slide(5)">Next</a>
                      </div>
                    </div>
                  </div>
                </div>
                </div>
                <div class="card walkthrough-card bg-dark text-white walkthrough-5">
                <div class="card-header"><h4>Validation results save you time.</h4></div>
                <div class="card-body">
                  <div class="image-sizer text-center">
                      <img src="../../../../../static/images/validation_failed_unexpected_values.gif" class="img-fluid rounded-sm mx-auto">
                  </div>
                  <p>This is an example of what a single failed Expectation looks like in Data Docs. Note the failure includes unexpected values from your data. This helps you debug pipelines faster.</p>
                </div>
                <div class="card-footer walkthrough-links">
                  <div class="container">
                    <div class="row">
                      <div class="col-sm">
                        <a href="#" class="next-link btn btn-secondary float-left" onclick="go_to_slide(4)">Back</a>
                      </div>
                      <div class="col-6 text-center text-secondary">
                        5 of 7
                        <div class="progress" style="height: 2px">
                          <div class="progress-bar bg-info" role="progressbar" style="width: 84%" aria-valuenow="84" aria-valuemin="0" aria-valuemax="84"></div>
                        </div>
                      </div>
                      <div class="col-sm">
                        <a href="#" class="next-link btn btn-primary float-right" onclick="go_to_slide(6)">Next</a>
                      </div>
                    </div>
                  </div>
                </div>
                </div>
                <div class="card walkthrough-card bg-dark text-white walkthrough-6">
                <div class="card-header"><h4>Great Expectations provides a large library of expectations.</h4></div>
                <div class="card-body">
                  <div class="image-sizer text-center">
                      <img src="../../../../../static/images/glossary_scroller.gif" class="img-fluid rounded-sm mx-auto">
                  </div>
                  <p><a href="https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=walkthrough&utm_medium=glossary">Nearly 50 built in expectations</a> allow you to express how you understand your data, and you can add custom
                  expectations if you need a new one.
                  </p>
                </div>
                <div class="card-footer walkthrough-links">
                  <div class="container">
                    <div class="row">
                      <div class="col-sm">
                        <a href="#" class="next-link btn btn-secondary float-left" onclick="go_to_slide(5)">Back</a>
                      </div>
                      <div class="col-6 text-center text-secondary">
                        6 of 7
                        <div class="progress" style="height: 2px">
                          <div class="progress-bar bg-info" role="progressbar" style="width: 84%" aria-valuenow="84" aria-valuemin="0" aria-valuemax="84"></div>
                        </div>
                      </div>
                      <div class="col-sm">
                        <a href="#" class="next-link btn btn-primary float-right" onclick="go_to_slide(7)">Next</a>
                      </div>
                    </div>
                  </div>
                </div>
                </div>
                <div class="card walkthrough-card bg-dark text-white walkthrough-7">
                <div class="card-header"><h4>Now explore and edit the sample suite!</h4></div>
                <div class="card-body">
                  <p>This sample suite shows you a few examples of expectations.</p>
                  <p>Note this is <strong>not a production suite</strong> and was generated using only a small sample of your data.</p>
                  <p>When you are ready, press the <strong>How to Edit</strong> button to kick off the iterative dev loop.</p>
                </div>
                <div class="card-footer walkthrough-links">
                  <div class="container">
                    <div class="row">
                      <div class="col-sm">
                        <a href="#" class="next-link btn btn-secondary float-left" onclick="go_to_slide(6)">Back</a>
                      </div>
                      <div class="col-6 text-center text-secondary">
                        7 of 7
                        <div class="progress" style="height: 2px">
                          <div class="progress-bar bg-info" role="progressbar" style="width: 100%" aria-valuenow="100" aria-valuemin="0" aria-valuemax="100"></div>
                        </div>
                      </div>
                      <div class="col-sm">
                        <button type="button" class="btn btn-primary float-right" data-dismiss="modal" aria-label="Close">
                          <span aria-hidden="true">Done</span>
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
                </div>
            </div>
        </div>
    </div>
</div>""", "")

    data_docs = data_docs.replace('<li class="ge-breadcrumbs-item breadcrumb-item"><a href="../../../../../index.html">Home</a></li>', '')
    data_docs = data_docs.replace('Expectation Validation Result', '')
    data_docs = data_docs.replace('Evaluates whether a batch of data matches expectations.', '')

    # Traduções
    data_docs = data_docs.replace('Table of Contents', 'Índice')
    data_docs = data_docs.replace('Actions', 'Ações')
    data_docs = data_docs.replace('Validation Filter', 'Filtro de indicador')
    data_docs = data_docs.replace('Show All', 'Mostrar tudo')
    data_docs = data_docs.replace('Failed Only', 'Apenas com falha')
    data_docs = data_docs.replace('Statistics', 'Estatisticas')
    data_docs = data_docs.replace('Overview', 'Visão geral')
    data_docs = data_docs.replace('Table-Level Expectations', 'Indicadores a nível de tabela')
    data_docs = data_docs.replace('Evaluated Expectations', 'Indicadores avaliados')
    data_docs = data_docs.replace('Successful Expectations', 'Indicadores bem sucedidos')
    data_docs = data_docs.replace('Unsuccessful Expectations', 'Indicadores mal sucedidos')
    data_docs = data_docs.replace('Success Percent', 'Percentual de sucesso')
    data_docs = data_docs.replace('Expectation', 'Indicador')
    data_docs = data_docs.replace('Show more info...', 'Ver mais informações...')
    data_docs = data_docs.replace('Observed Value', 'Valor Observado')
    data_docs = data_docs.replace('Search', 'Busca')

    f = open(data_docs_output_name, 'w', encoding="utf-8")
    f.write(data_docs)
    f.close()
    
    return None