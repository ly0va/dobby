use juniper::{graphql_object, EmptyMutation, EmptySubscription, RootNode};
use std::{collections::HashMap, sync::Arc};
use warp::Filter;

use crate::core::{
    types::{Query as DbQuery, TypedValue},
    Database,
};

struct Context(Arc<dyn Database>);
impl juniper::Context for Context {}

struct Car {
    id: i64,
    model: String,
    price: f64,
    owner: i64,
}

#[graphql_object(context = Context)]
impl Car {
    fn id(&self) -> i32 {
        self.id as i32
    }

    fn model(&self) -> &str {
        &self.model
    }

    fn price(&self) -> f64 {
        self.price
    }

    fn owner(&self, context: &Context) -> Option<Person> {
        context
            .0
            .execute(DbQuery::Select {
                from: "people".to_string(),
                columns: Default::default(),
                conditions: [("id".to_string(), TypedValue::Int(self.owner))]
                    .into_iter()
                    .collect(),
            })
            .map(|result| {
                result
                    .into_iter()
                    .map(|row| Person {
                        id: row.get("id").unwrap().to_string().parse().unwrap(),
                        name: row.get("name").unwrap().to_string().parse().unwrap(),
                        age: row.get("age").unwrap().to_string().parse().unwrap(),
                    })
                    .next()
            })
            .unwrap_or_default()
    }
}

struct Person {
    id: i64,
    name: String,
    age: i64,
}

#[graphql_object(context = Context)]
impl Person {
    fn id(&self) -> i32 {
        self.id as i32
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn age(&self) -> i32 {
        self.age as i32
    }
}

struct Query;

impl Query {
    fn get_cars(conditions: HashMap<String, TypedValue>, context: &Context) -> Vec<Car> {
        context
            .0
            .execute(DbQuery::Select {
                from: "cars".to_string(),
                columns: Default::default(),
                conditions,
            })
            .map(|result| {
                result
                    .into_iter()
                    .map(|row| Car {
                        id: row.get("id").unwrap().to_string().parse().unwrap(),
                        model: row.get("model").unwrap().to_string().parse().unwrap(),
                        price: row.get("price").unwrap().to_string().parse().unwrap(),
                        owner: row.get("owner").unwrap().to_string().parse().unwrap(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn get_people(conditions: HashMap<String, TypedValue>, context: &Context) -> Vec<Person> {
        context
            .0
            .execute(DbQuery::Select {
                from: "people".to_string(),
                columns: Default::default(),
                conditions,
            })
            .map(|result| {
                result
                    .into_iter()
                    .map(|row| Person {
                        id: row.get("id").unwrap().to_string().parse().unwrap(),
                        name: row.get("name").unwrap().to_string().parse().unwrap(),
                        age: row.get("age").unwrap().to_string().parse().unwrap(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[graphql_object(context = Context)]
impl Query {
    fn people(context: &Context) -> Vec<Person> {
        Self::get_people(Default::default(), context)
    }

    fn cars(context: &Context) -> Vec<Car> {
        Self::get_cars(Default::default(), context)
    }

    fn car(id: i32, context: &Context) -> Option<Car> {
        Self::get_cars(
            [("id".to_string(), TypedValue::Int(id as i64))]
                .into_iter()
                .collect(),
            context,
        )
        .into_iter()
        .next()
    }

    fn person(id: i32, context: &Context) -> Option<Person> {
        Self::get_people(
            [("id".to_string(), TypedValue::Int(id as i64))]
                .into_iter()
                .collect(),
            context,
        )
        .into_iter()
        .next()
    }
}

type Schema = RootNode<'static, Query, EmptyMutation<Context>, EmptySubscription<Context>>;

fn schema() -> Schema {
    Schema::new(Query, EmptyMutation::new(), EmptySubscription::new())
}

pub fn graphql(
    db: Arc<dyn Database>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let context = warp::any().map(move || Context(Arc::clone(&db)));

    let query = warp::post()
        .and(warp::path("graphql"))
        .and(warp::path::end())
        .and(juniper_warp::make_graphql_filter(schema(), context.boxed()));

    let playground = warp::get()
        .and(warp::path("playground"))
        .and(juniper_warp::playground_filter("/graphql", None));

    let graphiql = warp::get()
        .and(warp::path("graphiql"))
        .and(juniper_warp::graphiql_filter("/graphql", None));

    query
        .or(playground)
        .or(graphiql)
        .with(warp::log("api::graphql"))
}
