# Contributing to PostgreSQL to ClickHouse CDC Pipeline

Thank you for your interest in contributing! This document provides guidelines for contributing to the CDC pipeline project.

## 🚀 Getting Started

### Prerequisites
- Docker and Docker Compose
- PostgreSQL with logical replication enabled
- Basic knowledge of CDC concepts
- Familiarity with ClickHouse and Kafka

### Development Setup
1. Clone the repository
2. Copy example configurations: `cp connectors/postgres-source.example.json connectors/postgres-source.json`
3. Update database connection details
4. Start the stack: `docker compose up -d --build`

## 📋 How to Contribute

### 1. Reporting Issues
- Use GitHub Issues for bug reports and feature requests
- Include detailed reproduction steps
- Provide connector configurations (sanitized)
- Include relevant logs and error messages

### 2. Feature Requests
- Describe the use case and business value
- Provide examples of expected behavior
- Consider backward compatibility

### 3. Code Contributions
- Fork the repository
- Create a feature branch: `git checkout -b feature/your-feature-name`
- Make your changes
- Test thoroughly
- Submit a pull request

## 🔧 Development Guidelines

### Code Style
- Use clear, descriptive variable names
- Add comments for complex logic
- Follow existing patterns in the codebase
- Keep configurations well-documented

### Testing
- Test with different PostgreSQL data types
- Verify CDC latency and performance
- Test schema evolution scenarios
- Validate error handling

### Documentation
- Update README.md for new features
- Add examples for new configurations
- Update CHANGELOG.md
- Include inline code comments

## 📁 Project Structure

```
cdc/
├── docker-compose.yml          # Main infrastructure
├── connectors/                 # Kafka Connect configs
├── scripts/                    # Automation and utilities
├── clickhouse/                 # ClickHouse-specific files
├── docs/                       # Additional documentation
└── examples/                   # Usage examples
```

## 🧪 Testing Guidelines

### Unit Testing
- Test individual components
- Mock external dependencies
- Validate data transformations

### Integration Testing
- End-to-end CDC flow testing
- Multi-table scenarios
- Error recovery testing
- Performance benchmarking

### Test Data
- Use representative PostgreSQL schemas
- Test with various data types
- Include edge cases and null values
- Test large datasets for performance

## 📊 Performance Considerations

### Optimization Areas
- JSON parsing efficiency in materialized views
- Kafka partition strategies
- ClickHouse table engine selection
- Memory usage optimization

### Monitoring
- Add metrics for new features
- Monitor CDC latency
- Track error rates
- Measure throughput

## 🔒 Security Guidelines

### Sensitive Data
- Never commit credentials or secrets
- Use environment variables for configuration
- Sanitize logs and error messages
- Follow principle of least privilege

### Network Security
- Document required network access
- Use secure connection methods
- Implement proper authentication

## 📝 Pull Request Process

### Before Submitting
1. Ensure all tests pass
2. Update documentation
3. Add changelog entry
4. Verify backward compatibility

### PR Description Template
```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests pass
- [ ] Manual testing completed

## Checklist
- [ ] Code follows project style
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Changelog updated
```

## 🏷️ Versioning

We use [Semantic Versioning](https://semver.org/):
- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

## 📚 Resources

### Documentation
- [Debezium Documentation](https://debezium.io/documentation/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)

### Community
- GitHub Discussions for questions
- Issues for bug reports
- Pull requests for contributions

## 🎯 Contribution Areas

### High Priority
- Performance optimizations
- Additional data type support
- Error handling improvements
- Monitoring enhancements

### Medium Priority
- Schema evolution features
- Multi-database support
- Advanced transformations
- Security enhancements

### Documentation
- Usage examples
- Troubleshooting guides
- Performance tuning tips
- Best practices

## 🤝 Code of Conduct

- Be respectful and inclusive
- Provide constructive feedback
- Help others learn and grow
- Focus on technical merit

## 📞 Getting Help

- Check existing documentation first
- Search GitHub Issues
- Ask questions in Discussions
- Provide detailed context when asking for help

Thank you for contributing to making CDC pipelines better! 🚀
