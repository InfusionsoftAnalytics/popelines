import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(name='popelines',
      version='0.1.14',
      description='An ETL library for Google BigQuery',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/InfusionsoftAnalytics/popelines',
      author='Daniel Francis',
      author_email='daniel.francis@infusionsoft.com',
      license='MIT',
      zip_safe=False,
      packages=setuptools.find_packages(),
      classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent"]
)