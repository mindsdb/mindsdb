# UNDER CONSTRUCTION !

light_metadata = {
  "name": {
    "type": "string"
    },
  "version": {
    "type": "string"
    },
  "data_preparation": {
    "type": "object",
    "properties": {
        "accepted_margin_of_error": {
          "type": "number"
        },
        "total_row_count": {
            "type": "number"
        },
        "used_row_count": {
            "type": "number"
        },
        "test_row_count": {
            "type": "number"
        },
        "train_row_count": {
            "type": "number"
        },
        "validation_row_count": {
            "type": "number"
        }
    }
  },
  "data_analysis": {
    "type": "object",
    "properties": {
        "target_columns_metadata": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "column_name": {
                        "type": "string"
                    }
                }
            }
        }
    }
  }
}

scores = ['duplicates_score','empty_cells_score','data_type_distribution_score',
'similarity_score','z_test_based_outlier_score','value_distribution_score'
,'variability_score','redundancy_score','consistency_score','consistency_score','quality_score']

def gen_score(score_name):
    return [
        score_name: {
            "type": "object",
            "properties": {
                "score": {
                    "type": "number"
                },
                "description": {
                    "type": "string"
                }
            }
        }
    ]



  "data_analysis": {
    "target_columns_metadata": [
      {
        "column_name": "string",
        "importance_score": 0,
        "data_type": "categorical",
        "data_type_distribution": {
          "type": "categorical",
          "x": [
            "string"
          ],
          "y": [
            0
          ]
        },
        "data_distribution": {
          "data_histogram": {
            "type": "categorical",
            "x": [
              "string"
            ],
            "y": [
              0
            ]
          },
          "clusters": [
            {
              "group": "string",
              "members": [
                "string"
              ]
            }
          ],
          "mean": "string"
        },
        "consistency": {
          "score": "string",
          "metrics": [
            {
              "type": "error",
              "score": 0,
              "description": "string"
            }
          ],
          "description": "string"
        },
        "completeness": {
          "score": "string",
          "metrics": [
            {
              "type": "error",
              "score": 0,
              "description": "string"
            }
          ],
          "description": "string"
        },
        "variability": {
          "score": "string",
          "metrics": [
            {
              "type": "error",
              "score": 0,
              "description": "string"
            }
          ],
          "description": "string"
        }
      }
    ],
    "input_columns_metadata": [
      {
        "column_name": "string",
        "importance_score": 0,
        "data_type": "categorical",
        "data_type_distribution": {
          "type": "categorical",
          "x": [
            "string"
          ],
          "y": [
            0
          ]
        },
        "data_distribution": {
          "data_histogram": {
            "type": "categorical",
            "x": [
              "string"
            ],
            "y": [
              0
            ]
          },
          "clusters": [
            {
              "group": "string",
              "members": [
                "string"
              ]
            }
          ],
          "mean": "string"
        },
        "consistency": {
          "score": "string",
          "metrics": [
            {
              "type": "error",
              "score": 0,
              "description": "string"
            }
          ],
          "description": "string"
        },
        "completeness": {
          "score": "string",
          "metrics": [
            {
              "type": "error",
              "score": 0,
              "description": "string"
            }
          ],
          "description": "string"
        },
        "variability": {
          "score": "string",
          "metrics": [
            {
              "type": "error",
              "score": 0,
              "description": "string"
            }
          ],
          "description": "string"
        }
      }
    ]
  },
  "model_analysis": [
    {
      "column_name": "string",
      "overall_input_importance": {
        "type": "categorical",
        "x": [
          "string"
        ],
        "y": [
          0
        ]
      },
      "train_accuracy_over_time": {
        "type": "categorical",
        "x": [
          "string"
        ],
        "y": [
          0
        ]
      },
      "test_accuracy_over_time": {
        "type": "categorical",
        "x": [
          "string"
        ],
        "y": [
          0
        ]
      },
      "accuracy_histogram": {
        "x": [
          "string"
        ],
        "y": [
          0
        ],
        "x_explained": [
          [
            {
              "column_name": "string",
              "importance_score": 0,
              "data_type": "categorical",
              "data_type_distribution": {
                "type": "categorical",
                "x": [
                  "string"
                ],
                "y": [
                  0
                ]
              },
              "data_distribution": {
                "data_histogram": {
                  "type": "categorical",
                  "x": [
                    "string"
                  ],
                  "y": [
                    0
                  ]
                },
                "clusters": [
                  {
                    "group": "string",
                    "members": [
                      "string"
                    ]
                  }
                ],
                "mean": "string"
              },
              "consistency": {
                "score": "string",
                "metrics": [
                  {
                    "type": "error",
                    "score": 0,
                    "description": "string"
                  }
                ],
                "description": "string"
              },
              "completeness": {
                "score": "string",
                "metrics": [
                  {
                    "type": "error",
                    "score": 0,
                    "description": "string"
                  }
                ],
                "description": "string"
              },
              "variability": {
                "score": "string",
                "metrics": [
                  {
                    "type": "error",
                    "score": 0,
                    "description": "string"
                  }
                ],
                "description": "string"
              }
            }
          ]
        ]
      }
    }
  ]
})
