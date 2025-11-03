import { Paper } from './types';

export const MOCK_PAPERS: Paper[] = [
    {
        id: '1',
        title: 'Investigation of Random Laser in the Machine Learning Approach',
        authors: 'E. Santos',
        date: 'Nov. 2023',
        abstract: 'Machine Learning and Deep Learning are computational tools that fall within the domain of artificial intelligence. In recent years, numerous research works have advanced the application of machine and deep learning in various fields, including optics and photonics. In this...',
        categories: ['physics.optics'],
        url: 'https://arxiv.org/abs/2311.18579',
        source: 'research',
    },
    {
        id: '2',
        title: 'Machine Learning-enhanced Efficient Spectroscopic Ellipsometry Modeling',
        authors: 'A. Arunachalam',
        date: 'Jan. 2022',
        abstract: 'Over the recent years, there has been an extensive adoption of Machine Learning (ML) in a plethora of real-world applications, ranging from computer vision to data mining and drug discovery. In this paper, we utilize ML to facilitate efficient film fabrication, specifically...',
        categories: ['cond-mat.mtrl-sci', 'cs.LG'],
        url: 'https://arxiv.org/abs/2201.12061',
        source: 'research',
    },
    {
        id: '3',
        title: 'Machine Learning Regression of extinction in the second Gaia Data Release',
        authors: 'Y. Bai',
        date: 'Dec. 2019',
        abstract: 'Machine learning has become a popular tool to help us make better decisions and predictions, based on experiences, observations and analysing patterns within a given data set without explicitly functions. In this paper, we describe an application of the supervised...',
        categories: ['astro-ph.EP', 'astro-ph.SR'],
        url: 'https://arxiv.org/abs/1912.04987',
        source: 'research',
    }
];